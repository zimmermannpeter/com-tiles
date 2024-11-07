export interface TileRecord {
    zoom: number;
    column: number;
    row: number;
    data: Uint8Array;
}

export interface TileInfoRecord extends Omit<TileRecord, "data"> {
    size: number;
}
