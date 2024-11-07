import { fragmentSettings } from "@com-tiles/provider";
import { toBytesLE } from "./utils";

export function encodeIndexFragmentBitAligned(absoluteOffset: number, tileSizes: number[]): Buffer {
    const indexFragmentBitLength =
        fragmentSettings.indexOffsetBitWidth +
        tileSizes.length * fragmentSettings.bitAlignedFragment.indexEntryBitWidth;

    const indexFragmentByteLength = Math.ceil(indexFragmentBitLength / 8);
    const indexFragmentBuffer = Buffer.alloc(indexFragmentByteLength);

    const absoluteOffsetBuffer = toBytesLE(absoluteOffset, fragmentSettings.indexOffsetByteWidth);
    indexFragmentBuffer.fill(absoluteOffsetBuffer, 0, fragmentSettings.indexOffsetByteWidth);

    let bitCounter = fragmentSettings.indexOffsetBitWidth;

    for (let i = 0; i < tileSizes.length; i++) {
        const tileSize = tileSizes[i];

        if (tileSize > fragmentSettings.bitAlignedFragment.maxTileSize) {
            throw new Error(
                `Only tile size up to ${fragmentSettings.bitAlignedFragment.maxTileSize} is supported xin the current implementation.`,
            );
        }

        const byteStartIndex = Math.floor(bitCounter / 8);
        const bitOffset = bitCounter % 8;

        if (bitOffset === 0) {
            /* Little Endian Order 8 | 8 | 4 */
            const firstByte = indexFragmentBuffer[byteStartIndex] | ((tileSize << 4) & 0xff);
            indexFragmentBuffer.writeUint8(firstByte, byteStartIndex);

            const secondByte = (tileSize >> 4) & 0xff;
            indexFragmentBuffer.writeUint8(secondByte, byteStartIndex + 1);

            const thirdByte = tileSize >> 12;
            indexFragmentBuffer.writeUint8(thirdByte, byteStartIndex + 2);
        } else {
            /* Big Endian Order 4 | 8 | 8 */
            const firstByte = tileSize & 0xff;
            indexFragmentBuffer.writeUint8(firstByte, byteStartIndex);

            const secondByte = (tileSize >> 8) & 0xff;
            indexFragmentBuffer.writeUint8(secondByte, byteStartIndex + 1);

            const thirdByte = tileSize >> 16;
            indexFragmentBuffer.writeUint8(thirdByte, byteStartIndex + 2);
        }

        bitCounter += fragmentSettings.bitAlignedFragment.indexEntryBitWidth;
    }

    return indexFragmentBuffer;
}

//TODO: refactor -> Quick and dirty test implementation
export function encodeFragmentByteAligned(absoluteOffset: number, tileSizes: number[]): Buffer {
    const fragmentBitLength = fragmentSettings.indexOffsetBitWidth + tileSizes.length * 24;
    const fragmentByteLength = Math.ceil(fragmentBitLength / 8);
    const fragmentBuffer = Buffer.alloc(fragmentByteLength);

    const absoluteOffsetBuffer = toBytesLE(absoluteOffset, fragmentSettings.indexOffsetByteWidth);
    fragmentBuffer.fill(absoluteOffsetBuffer, 0, fragmentSettings.indexOffsetByteWidth);

    let fragmentBufferIndex = fragmentSettings.indexOffsetByteWidth;
    for (let i = 0; i < tileSizes.length; i++) {
        const tileSize = tileSizes[i];
        const firstByte = tileSize & 0xff;
        fragmentBuffer.writeUint8(firstByte, fragmentBufferIndex++);
        const secondByte = (tileSize >> 8) & 0xff;
        fragmentBuffer.writeUint8(secondByte, fragmentBufferIndex++);
        const thirdByte = tileSize >> 16;
        fragmentBuffer.writeUint8(thirdByte, fragmentBufferIndex++);
    }

    return fragmentBuffer;
}
