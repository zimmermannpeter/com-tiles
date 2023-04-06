import { Metadata } from "@com-tiles/spec";
import { TileMatrix } from "@com-tiles/spec/types/tileMatrix";

type TileMatrixLimits = TileMatrix["tileMatrixLimits"];

export interface FragmentRange {
    fragmentIndex: number;
    startOffset: number;
    endOffset: number;
}

export default class ComtIndex {
    private static readonly NUM_BYTES_ABSOLUTE_OFFSET = 5; // --> max size of index: 1 TB
    private static readonly NUM_BYTES_TILE_SIZE = 3; // --> maybe use 24 bits for later bit packing
    private static readonly Supported_TILE_MATRIX_CRS = "WebMercatorQuad";
    private static readonly SUPPORTED_ORDERING = [undefined, "RowMajor"];
    private readonly tileMatrixSet: Metadata["tileMatrixSet"];

    constructor(private readonly metadata: Metadata) {
        this.tileMatrixSet = metadata.tileMatrixSet;
        if (
            ![this.tileMatrixSet.fragmentOrdering, this.tileMatrixSet.tileOrdering].every((ordering) =>
                ComtIndex.SUPPORTED_ORDERING.some((o) => o === ordering),
            )
        ) {
            throw new Error(`The only supported fragment and tile ordering is ${ComtIndex.SUPPORTED_ORDERING}`);
        }

        if (
            this.tileMatrixSet.tileMatrixCRS !== undefined &&
            this.tileMatrixSet?.tileMatrixCRS.trim().toLowerCase() !== ComtIndex.Supported_TILE_MATRIX_CRS.toLowerCase()
        ) {
            throw new Error(`The only supported TileMatrixCRS is ${ComtIndex.Supported_TILE_MATRIX_CRS}.`);
        }
    }

    //TODO: add comment -> only range within the fragment of the data section
    getFragmentRangeForTile(
        zoom: number,
        x: number,
        y: number,
        metadataLength: number,
        pyramidLength: number,
    ): FragmentRange {
        //TODO: throw exception if zoom < fragment index
        const filteredSet = this.tileMatrixSet.tileMatrix.filter(
            (t) => t.aggregationCoefficient != -1 && t.zoom <= zoom,
        );

        let fragmentIndex = 0;
        let startOffset = 0;
        let endOffset = 0;

        for (const ts of filteredSet) {
            const limit = ts.tileMatrixLimits;
            if (ts.zoom === zoom && !this.inRange(x, y, limit)) {
                throw new Error("Specified tile index not part of the TileSet.");
            }
            if (ts.zoom < zoom) {
                const numTilesPerZoom =
                    (limit.maxTileCol - limit.minTileCol + 1) * (limit.maxTileRow - limit.minTileRow + 1);
                const numFragmentsPerZoom = this.calculateNumberFragmentsPerZoom(limit, ts.aggregationCoefficient);
                fragmentIndex += numFragmentsPerZoom;
                startOffset += numTilesPerZoom * 3 + numFragmentsPerZoom * 5;
            } else {
                /*
                 * 1. Calculate the number of index entries which are on the left side of the fragment
                 * 2. Calculate the number of index entries which are below the the fragment of the specified tile
                 * 3. Calculate the number of index entries in the fragment for the end offset
                 * ┌──────────────────────────────────────────────────────────┐
                 * │ ↑            ┆ fragment                                  │
                 * │ │            ├────────────────────────────────┐          │
                 * │ │            │ ↑                              │          │
                 * │ │            │ │                              │          │
                 * │ │            │ │                              │          │
                 * │ │     1      │ │             3                │          │
                 * │ │            │ ↓←────────────────────────────→│          │
                 * │ │            ├────────────────────────────────┴----------┤
                 * │ │            ┆↑                                          │
                 * │ │            ┆│                      2                   │
                 * │ ↓←──────────→┆↓←────────────────────────────────────────→│
                 * └──────────────┴───────────────────────────────────────────┘
                 */
                const sparseFragmentBounds = this.calculateBounds(ts.aggregationCoefficient, limit, x, y);
                const numIndexEntriesBeforeFragment = this.numberOfIndexEntriesBeforeFragment(
                    sparseFragmentBounds,
                    limit,
                );

                const numIndexEntriesInFragment =
                    (sparseFragmentBounds.maxTileCol - sparseFragmentBounds.minTileCol + 1) *
                    (sparseFragmentBounds.maxTileRow - sparseFragmentBounds.minTileRow + 1);

                //TODO: check -> -1 should be not needed
                const numFragmentsBefore =
                    this.calculateNumberOfFragmentsBefore(x, y, limit, ts.aggregationCoefficient) - 1;
                startOffset +=
                    numFragmentsBefore * 5 + numIndexEntriesBeforeFragment * 3 + metadataLength + pyramidLength;
                endOffset = startOffset + numIndexEntriesInFragment * 3 + 5;
            }
        }
        return { fragmentIndex, startOffset, endOffset };
    }

    /*
     * Calculates the offset for the specified tile in the index
     */
    calculateIndexOffsetForTile(zoom: number, x: number, y: number): { offset: number; index: number } {
        const offset = this.tileMatrixSet.tileMatrix
            .filter((tm) => tm.zoom <= zoom)
            .reduce((offset, ts) => {
                const limit = ts.tileMatrixLimits;
                if (ts.zoom === zoom && !this.inRange(x, y, limit)) {
                    throw new Error("Specified tile index not part of the TileSet");
                }
                if (this.isPyramidSection(ts.zoom, zoom)) {
                    const numTiles =
                        (limit.maxTileCol - limit.minTileCol + 1) * (limit.maxTileRow - limit.minTileRow + 1);
                    return offset + numTiles * ComtIndex.NUM_BYTES_TILE_SIZE;
                } else {
                    if (ts.aggregationCoefficient === -1) {
                        return this.calculatePyramidIndexOffsetForTile(
                            limit,
                            ts.aggregationCoefficient,
                            x,
                            y,
                            zoom,
                            offset,
                        );
                    } else {
                        return this.calculateFragmentIndexOffsetForTile(
                            limit,
                            ts.aggregationCoefficient,
                            x,
                            y,
                            zoom,
                            offset,
                        );
                    }
                }
            }, 0);

        const index = offset / ComtIndex.NUM_BYTES_TILE_SIZE;
        return { offset, index };
    }

    calculatePyramidIndexOffsetForTile(
        limit: TileMatrixLimits,
        aggregationCoefficient: number,
        x: number,
        y: number,
        zoom: number,
        offset: number,
    ): number {
        const numRows = y - limit.minTileRow;
        const numCols = limit.maxTileCol - limit.minTileCol + 1;
        const deltaCol = x - limit.minTileCol;
        return offset + (numRows * numCols + deltaCol) * ComtIndex.NUM_BYTES_TILE_SIZE;
    }

    calculateFragmentIndexOffsetForTile(
        limit: TileMatrixLimits,
        aggregationCoefficient: number,
        x: number,
        y: number,
        zoom: number,
        offset: number,
    ): number {
        /*
         * dataset
         * ┌──────────────────────────────────────────────────────────┐
         * │ ↑            ┆ fragment                                  │
         * │ │            ├──────────────────────┬───┬─────┐          │
         * │ │            │ ←─────── 4 ────────→ │ x │     │          │
         * │ │            ├----------------------┴───┴-----│          │
         * │ │            │ ↑                              │          │
         * │ │     1      │ │             3                │          │
         * │ │            │ ↓←────────────────────────────→│          │
         * │ │            ├────────────────────────────────┴----------┤
         * │ │            ┆↑                                          │
         * │ │            ┆│                      2                   │
         * │ ↓←──────────→┆↓←────────────────────────────────────────→│
         * └──────────────┴───────────────────────────────────────────┘
         *
         * Calculate the number of index entries before the fragment which contains the specified tile
         * 1. Calculate the number of index entries which are on the left side of the fragment
         * 2. Calculate the number of index entries which are below the the fragment of the specified tile
         * Calculate the number of index entries before the specified tile in the fragment
         * 3. Calculate the number of index entries for the full rows in the fragment which contains the specified tile
         * 4. Calculate the number of index entries before the specified tile in the partial row of the fragment
         */
        const sparseFragmentBounds = this.calculateBounds(aggregationCoefficient, limit, x, y);

        /* Step 1 & Step 2 */
        const numIndexEntriesBeforeFragment = this.numberOfIndexEntriesBeforeFragment(sparseFragmentBounds, limit);

        /* Step 3: Calculate the number of index entries for the full rows in the fragment which contains the specified tile */

        const fullRows =
            (y - sparseFragmentBounds.minTileRow) *
            (sparseFragmentBounds.maxTileCol - sparseFragmentBounds.minTileCol + 1);

        /* Step 4: Calculate the number of index entries before the specified tile in the partial row of the fragment */
        const partialRow = x - sparseFragmentBounds.minTileCol;

        const numIndexEntries = numIndexEntriesBeforeFragment + fullRows + partialRow;
        return offset + numIndexEntries * ComtIndex.NUM_BYTES_TILE_SIZE;
    }

    private calculateNumberFragmentsPerZoom(limit: TileMatrixLimits, aggregationCoefficient: number): number {
        const numTilesPerFragmentSide = 2 ** aggregationCoefficient;
        const minTileColFragmentIndex = Math.floor(limit.minTileCol / numTilesPerFragmentSide);
        const minTileRowFragmentIndex = Math.floor(limit.minTileRow / numTilesPerFragmentSide);
        const maxTileColFragmentIndex = Math.floor(limit.maxTileCol / numTilesPerFragmentSide);
        const maxTileRowFragmentIndex = Math.floor(limit.maxTileRow / numTilesPerFragmentSide);

        /*
         * 0-0,0-0 -> 1
         * 0-1,0-1 -> 4
         * 1-4, 1-3 -> 12
         * */
        //TODO: refactor and write test
        return (
            (maxTileColFragmentIndex - minTileColFragmentIndex + 1) *
            (maxTileRowFragmentIndex - minTileRowFragmentIndex + 1)
        );
    }

    private numberOfIndexEntriesBeforeFragment(
        sparseFragmentBounds: TileMatrixLimits,
        limit: TileMatrixLimits,
    ): number {
        /* Step 1: Calculate the number of tiles which are on the left side of the fragment */
        const numTilesLeftBeforeFragment =
            (sparseFragmentBounds.minTileCol - limit.minTileCol) *
            (sparseFragmentBounds.maxTileRow - limit.minTileRow + 1);

        /* Step 2: Calculate the number of tiles which are below the fragment of the specified tile */
        const numTilesLowBeforeFragment =
            (limit.maxTileCol - sparseFragmentBounds.minTileCol + 1) *
            (sparseFragmentBounds.minTileRow - limit.minTileRow);

        return numTilesLeftBeforeFragment + numTilesLowBeforeFragment;
    }

    private calculateNumberOfFragmentsBefore(
        x: number,
        y: number,
        denseFragmentsBounds: TileMatrixLimits,
        aggregationCoefficient: number,
    ) {
        /*
         * ┌────────────┬────────────────┬─────────┐
         * │ ↑          │                │         │
         * │ │    2     │       T        │         │
         * │ ↓ ←───────→│                │         │
         * ├------------┴────────────────┴---------┤
         * │↑                                      │
         * ││                 1                    │
         * │↓ ←───────────────────────────────────→│
         * └───────────────────────────────────────┘
         * */

        /* Step 1: Calculate the number fragments below */
        const numTilesPerFragmentSide = 2 ** aggregationCoefficient;
        const numFragmentsY = Math.floor((y - denseFragmentsBounds.minTileRow) / numTilesPerFragmentSide);
        const numFragmentsX = Math.round(
            (denseFragmentsBounds.maxTileCol - denseFragmentsBounds.minTileCol) / numTilesPerFragmentSide,
        );
        const numFragmentsActualRow = (numFragmentsX + 1) * (numFragmentsY + 1);

        /* Step 2: Calculate the number of fragments on the left */
        const numFragmentsActualCol = Math.floor((x - denseFragmentsBounds.minTileCol) / numTilesPerFragmentSide);

        return numFragmentsActualRow + numFragmentsActualCol;
    }

    private isPyramidSection(zoomTileSet: number, zoom: number): boolean {
        return zoomTileSet < zoom;
    }

    private inRange(x: number, y: number, tileSetLimits: TileMatrixLimits): boolean {
        return (
            x >= tileSetLimits.minTileCol &&
            x <= tileSetLimits.maxTileCol &&
            y >= tileSetLimits.minTileRow &&
            y <= tileSetLimits.maxTileRow
        );
    }

    private calculateBounds(
        aggregationCoefficient: number,
        limit: TileMatrixLimits,
        x?: number,
        y?: number,
    ): TileMatrixLimits {
        const numTilesPerFragmentSide = 2 ** aggregationCoefficient;
        const minTileColFragment =
            Math.floor((x || limit.minTileCol) / numTilesPerFragmentSide) * numTilesPerFragmentSide;
        const minTileRowFragment =
            Math.floor((y || limit.minTileRow) / numTilesPerFragmentSide) * numTilesPerFragmentSide;
        const denseLimits: TileMatrixLimits = {
            minTileCol: minTileColFragment,
            minTileRow: minTileRowFragment,
            maxTileCol: minTileColFragment + numTilesPerFragmentSide - 1,
            maxTileRow: minTileRowFragment + numTilesPerFragmentSide - 1,
        };

        if (x == null && y == null) {
            /*  calculateDenseFragmentBounds */
            return denseLimits;
        } else {
            /*  calculateSparseFragmentBounds */
            const intersectedLimits = { ...denseLimits };
            if (limit.minTileCol > denseLimits.minTileCol) {
                intersectedLimits.minTileCol = limit.minTileCol;
            }
            if (limit.minTileRow > denseLimits.minTileRow) {
                intersectedLimits.minTileRow = limit.minTileRow;
            }
            if (limit.maxTileCol < denseLimits.maxTileCol) {
                intersectedLimits.maxTileCol = limit.maxTileCol;
            }
            if (limit.maxTileRow < denseLimits.maxTileRow) {
                intersectedLimits.maxTileRow = limit.maxTileRow;
            }
            return intersectedLimits;
        }
    }
}
