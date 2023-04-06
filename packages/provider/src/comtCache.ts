import fflate from "fflate";
import { Metadata } from "@com-tiles/spec";
import ComtIndex, { FragmentRange } from "./comtIndex";
import LruCache from "./lruCache";
import { convertUInt24LEToNumber, convertUInt40LEToNumber, Optional } from "./utils";
import BatchRequestDispatcher, { TileRequest } from "./batchRequestDispatcher";
import { TmsIndex, XyzIndex } from "./tileIndex";
import CancellationToken from "./cancellationToken";

interface Header {
    indexOffset: number;
    pyramidIndexLength: number;
    fragmentIndexLength: number;
    dataOffset: number;
    metadata: Metadata;
    partialIndex: ArrayBuffer;
}

interface IndexEntry {
    offset: number;
    size: number;
}

class IndexCache {
    /*
     * The partial index is always kept in memory and can be mixed up with fragmented and unfragmented tile matrices.
     * For the index fragments which are added to the cache a LRU cache is used.
     * Through this procedure there can be redundant index entries in the partial index and the LRU cache
     * when the last fragment of the partial index is incomplete, but in general this doesn't matter.
     * */
    private static readonly INDEX_ENTRY_NUM_BYTES = 3;
    private static readonly FRAGMENT_ENTRY_NUM_BYTES = 5;
    private static readonly MAX_ENTRIES_LRU_CACHE = 28;
    private readonly fragmentedIndex = new LruCache<number, { fragmentRange: FragmentRange; indexEntries: Uint8Array }>(
        IndexCache.MAX_ENTRIES_LRU_CACHE,
    );
    private readonly comtIndex: ComtIndex;

    constructor(
        private readonly metadata: Metadata,
        private readonly partialIndex = new Uint8Array(0),
        private readonly indexOffset: number,
        private readonly pyramidIndexLength: number,
        private readonly fragmentIndexLength: number,
        private readonly cacheSize?: number,
    ) {
        this.comtIndex = new ComtIndex(this.metadata);
    }

    setIndexFragment(fragmentRange: FragmentRange, indexEntries: Uint8Array): void {
        const index = fragmentRange.startOffset;
        this.fragmentedIndex.put(index, { fragmentRange, indexEntries });
    }

    /**
     * @param tmsIndex Index of a specific tile in the TMS tiling scheme.
     * @returns Relative offset and size of the specified tile in the data section.
     */

    get(tmsIndex: TmsIndex): IndexEntry {
        const { z, x, y } = tmsIndex;
        console.log(tmsIndex);
        const { index: indexOffsetForTile } = this.comtIndex.calculateIndexOffsetForTile(z, x, y);
        console.log("indexOffsetForTile", indexOffsetForTile);
        const { fragmentIndex, startOffset } = this.comtIndex.getFragmentRangeForTile(
            z,
            x,
            y,
            this.indexOffset,
            this.pyramidIndexLength,
        );

        const indexOffset =
            z < 8 ? indexOffsetForTile : this.getIndexOffsetFragmentSection(fragmentIndex, indexOffsetForTile);

        if (indexOffset <= this.partialIndex.byteLength - IndexCache.INDEX_ENTRY_NUM_BYTES && z < 8) {
            return this.getIndexEntryPyramidSection(indexOffset, fflate.unzlibSync(this.partialIndex));
        }

        const indexFragment = this.fragmentedIndex.get(startOffset);

        if (!indexFragment) {
            return null;
        }

        //TODO: fix relativeFragmentOffset
        const relativeFragmentOffset = 0;

        return this.getIndexEntryFragmentSection(relativeFragmentOffset, indexFragment.indexEntries);
    }

    getIndexOffsetFragmentSection(fragmentIndex, indexOffsetForTile) {
        return (
            fragmentIndex * IndexCache.FRAGMENT_ENTRY_NUM_BYTES +
            (indexOffsetForTile - 1) * IndexCache.INDEX_ENTRY_NUM_BYTES
        );
    }

    private getIndexEntryFragmentSection(indexOffset: number, indexEntries: Uint8Array): IndexEntry {
        const indexBuffer = indexEntries.buffer;
        const size = convertUInt24LEToNumber(indexBuffer, indexOffset * IndexCache.INDEX_ENTRY_NUM_BYTES + 5);

        let offset = convertUInt40LEToNumber(indexBuffer, 0);
        for (let i = 0; i < indexOffset; i++) {
            offset += convertUInt24LEToNumber(indexBuffer, 5 + i * IndexCache.INDEX_ENTRY_NUM_BYTES);
        }
        return { offset, size };
    }

    private getIndexEntryPyramidSection(indexOffset: number, indexEntries: Uint8Array): IndexEntry {
        const indexBuffer = indexEntries.buffer;

        const size = convertUInt24LEToNumber(indexBuffer, indexOffset * IndexCache.INDEX_ENTRY_NUM_BYTES);
        let offset = 0;

        for (let i = 0; i < indexOffset; i++) {
            offset += convertUInt24LEToNumber(indexBuffer, i * IndexCache.INDEX_ENTRY_NUM_BYTES);
        }
        return { offset, size };
    }
}

export enum HeaderFetchStrategy {
    PREFETCH = "PREFETCH",
    LAZY = "LAZY ",
}

/*
 * The ComtCache class has currently the following limitations regarding the support of the COMTiles spec:
 * - The only supported TileMatrixCRS is WebMercatorQuad
 * - Only Mapbox vector tiles are supported as content of a map tile and no raster formats (PNG, WebP)
 * - The only supported space-filling curve type for the order of the index fragments and tiles is row-major
 * - Only index fragments can be loaded after the initial fetch.
 *   So with the first initial fetch all the unfragmented part of the index has to be fetched and can't be lazy loaded.
 * */
export default class ComtCache {
    private static readonly SUPPORTED_VERSION = 1;
    private static readonly INITIAL_CHUNK_SIZE = 2 ** 19; //512k
    private static readonly METADATA_OFFSET_INDEX = 24;
    private static readonly SUPPORTED_TILE_MATRIX_CRS = "WebMercatorQuad";
    private static readonly SUPPORTED_ORDERING = "RowMajor";
    private static readonly INDEX_ENTRY_NUM_BYTES = 3;
    private static readonly FRAGMENT_OFFSET_NUM_BYTES = 5;
    private indexCache: IndexCache = null;
    private comtIndex: ComtIndex = null;
    private readonly requestCache = new Map<number, Promise<ArrayBuffer>>();
    private headerLoaded: Promise<Header>;
    private readonly batchedTilesProvider: BatchRequestDispatcher;

    private constructor(private readonly comtUrl: string, private readonly throttleTime, private header?: Header) {
        if (header) {
            this.initIndex(header);
        }

        this.batchedTilesProvider = new BatchRequestDispatcher(comtUrl, throttleTime);
    }

    /**
     * @param comtUrl Url to object storage where the COMTiles archive is hosted.
     * @param prefetchHeader Specifies if the header should be prefetched or lazy loaded.
     * @param throttleTime Time to wait for batching up the tile requests.
     */
    static async create(
        comtUrl: string,
        prefetchHeader = HeaderFetchStrategy.PREFETCH,
        throttleTime = 5,
    ): Promise<ComtCache> {
        const header = prefetchHeader === HeaderFetchStrategy.PREFETCH ? await ComtCache.loadHeader(comtUrl) : null;
        return new ComtCache(comtUrl, throttleTime, header);
    }

    /**
     * @param comtUrl Url to object storage where the COMTiles archive is hosted.
     * @param throttleTime Time to wait for batching up the tile requests.
     */
    static createSync(comtUrl: string, throttleTime = 5): ComtCache {
        return new ComtCache(comtUrl, throttleTime);
    }

    /**
     * Fetches a map tile with the given XYZ index from the specified COMTiles archive.
     *
     * @param xyzIndex Index of the tile in the XYZ tiling scheme.
     * @param cancellationToken For aborting the tile request.
     */
    async getTile(xyzIndex: XyzIndex, cancellationToken?: CancellationToken): Promise<ArrayBuffer> {
        const provider = (index, indexEntry, absoluteTileOffset, cancellationToken) => {
            return this.fetchMVT(absoluteTileOffset, indexEntry.size, cancellationToken);
        };
        return this.fetchTile(xyzIndex, provider, cancellationToken);
    }

    /**
     * Tries to batch the tile requests by aggregating all requests within the given throttleTime.
     *
     * @param xyzIndex Index of the tile in the XYZ tiling scheme.
     * @param cancellationToken For aborting the tile request.
     */
    async getTileWithBatchRequest(xyzIndex: XyzIndex, cancellationToken?: CancellationToken): Promise<ArrayBuffer> {
        const provider = (index, indexEntry, absoluteTileOffset, cancellationToken) => {
            const tileRequest: TileRequest = {
                index: xyzIndex,
                range: {
                    startOffset: absoluteTileOffset,
                    endOffset: absoluteTileOffset + indexEntry.size - 1,
                },
            };

            return this.batchedTilesProvider.fetchTile(tileRequest, cancellationToken);
        };

        return this.fetchTile(xyzIndex, provider, cancellationToken);
    }

    private async fetchTile(
        xyzIndex: XyzIndex,
        tileProvider: (
            index: XyzIndex,
            indexEntry: IndexEntry,
            absoluteTileOffset: number,
            cancellationToken: CancellationToken,
        ) => Promise<ArrayBuffer>,
        cancellationToken?: CancellationToken,
    ): Promise<ArrayBuffer> {
        const optionalIndexEntry = await this.getIndexEntry(xyzIndex, cancellationToken);
        if (!optionalIndexEntry.isPresent()) {
            return new Uint8Array(0);
        }
        const { indexEntry, absoluteTileOffset } = optionalIndexEntry.get();

        /* Return an empty array if the tile is missing */
        return indexEntry.size
            ? tileProvider(xyzIndex, indexEntry, absoluteTileOffset, cancellationToken)
            : new Uint8Array(0);
    }

    private async getIndexEntry(
        xyzIndex: XyzIndex,
        cancellationToken,
    ): Promise<Optional<{ indexEntry: IndexEntry; absoluteTileOffset: number }>> {
        /* Lazy load the header on the first tile request */
        if (!this.header) {
            if (!this.headerLoaded) {
                this.headerLoaded = ComtCache.loadHeader(this.comtUrl);
                this.header = await this.headerLoaded;
                this.initIndex(this.header);
            } else {
                await this.headerLoaded;
            }
        }

        const { metadata } = this.header;
        const { x, y, z } = xyzIndex;
        /* COMTiles uses the y-axis alignment of the TMS spec which is flipped compared to xyz */
        const tmsY = (1 << z) - y - 1;
        const limit = metadata.tileMatrixSet.tileMatrix[z].tileMatrixLimits;
        if (x < limit.minTileCol || x > limit.maxTileCol || tmsY < limit.minTileRow || tmsY > limit.maxTileRow) {
            /* Requested tile not within the boundary ot the TileSet */
            return Optional.empty();
        }

        const tmsIndex = { z, x, y: tmsY };
        const indexEntry = this.indexCache.get(tmsIndex) ?? (await this.fetchIndexEntry(tmsIndex, cancellationToken));
        const absoluteTileOffset = this.header.dataOffset + indexEntry.offset;
        return Optional.of({ indexEntry, absoluteTileOffset });
    }

    private async fetchIndexEntry(tmsIndex: TmsIndex, cancellationToken: CancellationToken): Promise<IndexEntry> {
        const fragmentRange = this.comtIndex.getFragmentRangeForTile(
            tmsIndex.z,
            tmsIndex.x,
            tmsIndex.y,
            this.header.indexOffset,
            this.header.pyramidIndexLength,
        );

        let indexFragment: ArrayBuffer;
        /* avoid redundant requests to the same index fragment */
        if (!this.requestCache.has(fragmentRange.startOffset)) {
            //TODO: check if pyramid or fragment section
            const startOffset = fragmentRange.startOffset;
            const endOffset = fragmentRange.endOffset;
            const indexEntryRequest = ComtCache.fetchBinaryData(
                this.comtUrl,
                startOffset,
                endOffset,
                cancellationToken,
            );
            this.requestCache.set(fragmentRange.startOffset, indexEntryRequest);

            try {
                indexFragment = await indexEntryRequest;
            } finally {
                this.requestCache.delete(fragmentRange.startOffset);
            }
        } else {
            indexFragment = await this.requestCache.get(fragmentRange.startOffset);
        }

        this.indexCache.setIndexFragment(fragmentRange, new Uint8Array(indexFragment));
        return this.indexCache.get(tmsIndex);
    }

    private initIndex(header: Header): void {
        this.indexCache = new IndexCache(
            this.header.metadata,
            new Uint8Array(this.header.partialIndex),
            this.header.indexOffset,
            this.header.pyramidIndexLength,
            this.header.fragmentIndexLength,
        );
        this.comtIndex = new ComtIndex(header.metadata);
    }

    private static fetchHeader(comtUrl: string): Promise<ArrayBuffer> {
        return ComtCache.fetchBinaryData(comtUrl, 0, ComtCache.INITIAL_CHUNK_SIZE - 1);
    }

    private static async fetchBinaryData(
        url: string,
        firstBytePos: number,
        lastBytePos: number,
        cancellationToken?: CancellationToken,
    ): Promise<ArrayBuffer> {
        const requestOptions = {
            headers: {
                range: `bytes=${firstBytePos}-${lastBytePos}`,
            },
        };
        let abortRequest;
        if (cancellationToken) {
            const controller = new AbortController();
            const { signal, abort } = controller;
            Object.assign(requestOptions, { signal });
            abortRequest = abort.bind(controller);
            cancellationToken.register(abortRequest);
        }

        const response = await fetch(url, requestOptions);
        cancellationToken?.unregister(abortRequest);

        if (!response.ok) {
            throw new Error(response.statusText);
        }

        return response.arrayBuffer();
    }

    private static async loadHeader(comtUrl: string): Promise<Header> {
        const buffer = await ComtCache.fetchHeader(comtUrl);

        const view = new DataView(buffer);

        const version = view.getUint32(4, true);

        if (version !== ComtCache.SUPPORTED_VERSION) {
            throw new Error("The specified version of the COMT archive is not supported.");
        }

        const metadataSize = view.getUint32(8, true);
        const pyramidIndexLength = view.getUint32(12, true);
        const fragmentIndexLength = view.getUint32(16, true);
        const indexOffset = ComtCache.METADATA_OFFSET_INDEX + metadataSize;
        const metadataBuffer = buffer.slice(ComtCache.METADATA_OFFSET_INDEX, indexOffset);
        const metadataDocument = new TextDecoder().decode(metadataBuffer);
        const metadata = JSON.parse(metadataDocument);
        const numCompleteIndexEntries = Math.floor(
            (ComtCache.INITIAL_CHUNK_SIZE - indexOffset) / ComtCache.INDEX_ENTRY_NUM_BYTES,
        );

        this.validateMetadata(metadata, numCompleteIndexEntries);

        // const endOffset = indexOffset + numCompleteIndexEntries * ComtCache.INDEX_ENTRY_NUM_BYTES;
        const endOffset = indexOffset + pyramidIndexLength;
        const partialIndex = buffer.slice(indexOffset, endOffset);
        const dataOffset = indexOffset + pyramidIndexLength + fragmentIndexLength;
        console.log(
            `indexOffset: ${indexOffset}\npyramidIndexLength: ${pyramidIndexLength}\nfragmentIndexLength ${fragmentIndexLength}\ndataOffset: ${dataOffset}`,
        );
        return {
            indexOffset,
            pyramidIndexLength,
            fragmentIndexLength,
            dataOffset,
            metadata,
            partialIndex,
        };
    }

    private static validateMetadata(metadata: Metadata, downloadedPyramidIndexEntries: number): void {
        if (metadata.tileFormat !== "pbf") {
            throw new Error("Currently pbf (MapboxVectorTiles) is the only supported tileFormat.");
        }

        const tileMatrixSet = metadata.tileMatrixSet;
        const supportedOrdering = [undefined, ComtCache.SUPPORTED_ORDERING];

        if (
            ![tileMatrixSet.fragmentOrdering, tileMatrixSet.tileOrdering].every((ordering) =>
                supportedOrdering.some((o) => o === ordering),
            )
        ) {
            throw new Error(`The only supported fragment and tile ordering is ${ComtCache.SUPPORTED_ORDERING}`);
        }

        if (
            tileMatrixSet.tileMatrixCRS !== undefined &&
            tileMatrixSet?.tileMatrixCRS.trim().toLowerCase() !== ComtCache.SUPPORTED_TILE_MATRIX_CRS.toLowerCase()
        ) {
            throw new Error(`The only supported TileMatrixCRS is ${ComtCache.SUPPORTED_TILE_MATRIX_CRS}.`);
        }

        const pyramidIndexEntries = tileMatrixSet.tileMatrix
            .filter((tm) => tm.aggregationCoefficient === -1)
            .reduce((numIndexEntries, tm) => {
                const limits = tm.tileMatrixLimits;
                return (
                    numIndexEntries +
                    (limits.maxTileCol - limits.minTileCol + 1) * (limits.maxTileRow - limits.minTileRow + 1)
                );
            }, 0);
        /* Currently only index fragments can be loaded after the initial fetch */
        if (pyramidIndexEntries > downloadedPyramidIndexEntries) {
            throw new Error(
                "The fragmented part (aggregationCoefficient=-1) of the index has to be part of the initial fetch. Only index fragments can be reloaded",
            );
        }
    }

    private async fetchMVT(
        tileOffset: number,
        tileSize: number,
        cancellationToken: CancellationToken,
    ): Promise<Uint8Array> {
        const buffer = await ComtCache.fetchBinaryData(
            this.comtUrl,
            tileOffset,
            tileOffset + tileSize - 1,
            cancellationToken,
        );
        const compressedTile = new Uint8Array(buffer);
        // return pako.ungzip(compressedTile);
        return fflate.gunzipSync(compressedTile);
    }
}
