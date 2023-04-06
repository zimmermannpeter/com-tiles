#!/usr/bin/env node
import fs from "fs";
import { program } from "commander";
import { Metadata } from "@com-tiles/spec";
import { MBTilesRepository } from "./mbTilesRepository";
import { calculateNumTiles, toBytesLE } from "./utils";
import pkg from "../package.json";
import MapTileProvider, { RecordType } from "./tileProvider";
import Logger from "./logger";
import fflate from "fflate";
import { ComtIndex } from "@com-tiles/provider";

program
    .version(pkg.version)
    .option("-i, --inputFilePath <path>", "specify path and filename of the MBTiles database")
    .option("-o, --outputFilePath <path>", "specify path and filename of the COMT archive file")
    .option(
        "-z, --pyramidMaxZoom <number>",
        "specify to which zoom level a compressed pyramid for the index is used. Defaults to 7.",
    )
    .option(
        "-m, --maxZoomDbQuery <number>",
        "specify to which zoom level the TileMatrixLimits should be queried from the db and not calculated based on the bounds",
    )
    .parse(process.argv);

const options = program.opts();
if (!options.inputFilePath || !options.outputFilePath) {
    throw new Error("Please specify the inputFilePath and the outputFilePath.");
}

const MAGIC = "COMT";
const VERSION = 1;
const NUM_BYTES_ABSOLUTE_OFFSET = 5;
const NUM_BYTES_TILE_SIZE = 3;
const MAX_ZOOM_DB_QUERY = parseInt(options.maxZoomDbQuery) || 8;
const MAX_PYRAMID_ZOOM = parseInt(options.pyramidMaxZoom) || 7;
const FRAGMENT_AGGREGATION_COEFFICIENT_DEFAULT = 6;

/*
 * max size of a tile is limited to 1 mb --> maximum tile size needed for z14 is ~930 kB
 * idea: dynamic size depending on max needed size --> SELECT MAX(LENGTH(tile_data)) FROM tiles
 * query takes ~ 15 minutes on SSD
 */

const MAX_TILE_SIZE = (1 << 20) - 1;

(async () => {
    const logger = new Logger();
    const mbTilesFilename = options.inputFilePath;
    const comTilesFilename = options.outputFilePath;

    logger.info(`Converting the MBTiles file ${mbTilesFilename} to a COMTiles archive.`);
    await createComTileArchive(mbTilesFilename, comTilesFilename, MAX_PYRAMID_ZOOM, logger);
    logger.info(`Successfully saved the COMTiles archive in ${comTilesFilename}.`);
})();

async function createComTileArchive(
    mbTilesFilename: string,
    comTilesFileName: string,
    pyramidMaxZoom: number,
    logger: Logger,
) {
    const repo = await MBTilesRepository.create(mbTilesFilename);
    const metadata = await repo.getMetadata(pyramidMaxZoom, FRAGMENT_AGGREGATION_COEFFICIENT_DEFAULT);
    const filteredTileMatrixSet = metadata.tileMatrixSet.tileMatrix.filter(
        (tileMatrix) => tileMatrix.zoom <= MAX_ZOOM_DB_QUERY,
    );
    for (const tileMatrix of filteredTileMatrixSet) {
        tileMatrix.tileMatrixLimits = await repo.getTileMatrixLimits(tileMatrix.zoom);
    }

    const tileMatrixSet = metadata.tileMatrixSet;
    const metadataJson = JSON.stringify(metadata);

    const stream = fs.createWriteStream(comTilesFileName, {
        encoding: "binary",
        highWaterMark: 1_000_000,
    });

    logger.info("Writing the header and metadata to the COMTiles archive.");
    writeHeader(stream, metadataJson.length);
    writeMetadata(stream, metadataJson);

    const tileProvider = new MapTileProvider(repo, tileMatrixSet.tileMatrix);
    logger.info("Writing the index to the COMTiles archive.");
    console.time("writeIndex");
    const indexStats = await writeIndex(stream, tileProvider, metadata, pyramidMaxZoom);
    console.timeEnd("writeIndex");

    logger.info("Writing the map tiles to the the COMTiles archive.");
    console.time("writeTiles");
    await writeTiles(stream, tileProvider);
    console.timeEnd("writeTiles");

    stream.end();
    await repo.dispose();
    stream.end();

    const pyramidBuffer = Buffer.alloc(4);
    const fragmentBuffer = Buffer.alloc(8);

    pyramidBuffer.writeUInt32LE(indexStats[0]);
    fragmentBuffer.writeBigUint64LE(BigInt(indexStats[1]));

    fs.open(comTilesFileName, "r+", function (err, fd) {
        if (err) {
            console.log("Cant open file");
        } else {
            fs.writeSync(fd, pyramidBuffer, 0, pyramidBuffer.length, 12);
            fs.writeSync(fd, fragmentBuffer, 0, fragmentBuffer.length, 16);
        }
    });
}

/*
 * Structure of the header:
 * Magic (char[4]) | Version (uint32) | Metadata Length (uint32) | Index Pyramid Length (uint32) | Index Fragment Length (uint64)
 */

function writeHeader(stream: fs.WriteStream, metadataByteLength: number) {
    stream.write(MAGIC);

    const versionBuffer = Buffer.alloc(4);
    versionBuffer.writeUInt32LE(VERSION);

    stream.write(versionBuffer);

    const metadataLengthBuffer = Buffer.alloc(4);
    metadataLengthBuffer.writeUInt32LE(metadataByteLength);
    stream.write(metadataLengthBuffer);

    const pyramidIndexLength = Buffer.alloc(4);
    stream.write(pyramidIndexLength);
    const fragmentedIndexLength = Buffer.alloc(8);
    stream.write(fragmentedIndexLength);
}

function writeMetadata(stream: fs.WriteStream, metadataJson: string) {
    stream.write(metadataJson, "utf-8");
}

async function writeIndex(
    stream: fs.WriteStream,
    tileProvider: MapTileProvider,
    metadata: Metadata,
    pyramidMaxZoom = MAX_PYRAMID_ZOOM,
): Promise<[pyramidSectionLength: number, fragmentSectionLength: number]> {
    const comtIndex = new ComtIndex(metadata);
    const tiles = tileProvider.getTilesInRowMajorOrder(RecordType.SIZE);

    const pyramidSet = metadata.tileMatrixSet.tileMatrix.filter((set) => set.zoom <= pyramidMaxZoom);
    const numIndexEntriesPyramid = calculateNumTiles(pyramidSet);
    const tileIterator = tiles[Symbol.asyncIterator]();
    const indexPyramidBuffer = Buffer.alloc(numIndexEntriesPyramid * NUM_BYTES_TILE_SIZE);
    const logStreamPyramid = fs.createWriteStream("C:\\mapdata\\comtiles\\log_germany_pyramid.txt");
    const logStreamFragments = fs.createWriteStream("C:\\mapdata\\comtiles\\log_germany_fragments.txt");

    let dataSectionOffset = 0;

    for (let i = 0; i < numIndexEntriesPyramid; i++) {
        const tileInfo = (await tileIterator.next()).value;
        //TODO: also check for padding missing tiles
        const size = tileInfo.size;
        dataSectionOffset += size;
        logStreamPyramid.write(size + "\n");
        validateTileSize(size);
        toBytesLE(size, NUM_BYTES_TILE_SIZE).copy(indexPyramidBuffer, i * NUM_BYTES_TILE_SIZE);
    }
    const compressedIndexPyramid = fflate.zlibSync(indexPyramidBuffer);

    await writeBuffer(stream, compressedIndexPyramid);
    logStreamPyramid.end();

    /*
     * Magic | Version | Metadata Length | Index Tile Pyramid Length | Index Fragment Length | Metadata | Index | Data
     * -> Header is Magic, Version, Metadata Length, Pyramid Length and Fragment Length
     * -> Header | Metadata | Index | Data
     * -> because the tile pyramid is compressed we need the actual size
     * -> tile pyramid always has to be full zoom levels
     * -> tile pyramid is always fully fetched
     * - write compressed tile pyramid -> calculate pyramid size and write to the header
     * - write fragments with an absolute offset and only a size per index record
     * */

    let tile;
    let previousFragmentIndex = -1;
    let previousIndex = compressedIndexPyramid.length;
    let indexFragmentLength = 0;
    let previousZoom = -1;
    while (((tile = await tileIterator.next()), !tile.done)) {
        const { zoom, column, row, size, fragmentIndex } = tile.value;

        if (zoom > previousZoom) {
            console.log("start", zoom, "at", new Date().toLocaleTimeString());
            previousZoom = zoom;
        }

        validateTileSize(size);

        /* First tile in a new fragment  */
        if (fragmentIndex > previousFragmentIndex) {
            const absoluteFragmentOffset = toBytesLE(dataSectionOffset, NUM_BYTES_ABSOLUTE_OFFSET);
            await writeBuffer(stream, absoluteFragmentOffset);
            logStreamFragments.write("FragmentOffset " + dataSectionOffset + "\n");

            indexFragmentLength += NUM_BYTES_ABSOLUTE_OFFSET;
        }
        const { index: currentIndex } = comtIndex.calculateIndexOffsetForTile(zoom, column, row);
        const padding = currentIndex - previousIndex - 1;

        /* Add a padding in the index for the missing tiles in the MBTiles database */
        if (padding > 0) {
            for (let i = 0; i < padding; i++) {
                await writeBuffer(stream, toBytesLE(0, NUM_BYTES_TILE_SIZE));
                logStreamFragments.write("padding " + "\n");
                indexFragmentLength += NUM_BYTES_TILE_SIZE; // is this needed?
            }
        }
        const indexEntry = toBytesLE(size, NUM_BYTES_TILE_SIZE);
        await writeBuffer(stream, indexEntry);
        logStreamFragments.write("entry " + size + "\n");

        indexFragmentLength += NUM_BYTES_TILE_SIZE;
        dataSectionOffset += size;
        previousFragmentIndex = fragmentIndex;
        previousIndex = currentIndex;
    }
    logStreamFragments.end();
    console.log([compressedIndexPyramid.length, indexFragmentLength]);
    return [compressedIndexPyramid.length, indexFragmentLength];
}

function validateTileSize(tileSize: number): void {
    if (tileSize > MAX_TILE_SIZE) {
        throw new Error(`The maximum size of a tile is limited to ${MAX_TILE_SIZE / 1024 / 1024} Mb.`);
    }
}

async function writeTiles(stream: fs.WriteStream, tileProvider: MapTileProvider): Promise<void> {
    const tiles = tileProvider.getTilesInRowMajorOrder(RecordType.TILE);

    /* Batching the tile writes did not bring the expected performance gain because allocating the buffer
     * for the tile batches was too expensive. So we simplified again to single tile writes. */
    for await (const { data: tile } of tiles) {
        const tileLength = tile.byteLength;
        if (tileLength > 0) {
            const tileBuffer = Buffer.from(tile);
            await writeBuffer(stream, tileBuffer);
        }
    }
}

async function writeBuffer(stream: fs.WriteStream, buffer: Buffer | Uint8Array): Promise<void> {
    const canContinue = stream.write(buffer);
    if (!canContinue) {
        await new Promise((resolve) => {
            stream.once("drain", resolve);
        });
    }
}
