import fragmentSettings from "./fragmentSettings";
import { convertUInt40LEToNumber } from "./utils";

const firstByteLookupTable = new Uint8Array(4096);
const secondByteLookupTable = new Uint8Array(4096);
const thirdByteLookupTable = new Uint8Array(4096);
for (let i = 1; i <= 4096; i++) {
    if (i % 2 !== 0) {
        firstByteLookupTable[i] = 0;
        secondByteLookupTable[i] = 8;
        thirdByteLookupTable[i] = 16;
    } else {
        firstByteLookupTable[i] = 4;
        secondByteLookupTable[i] = 4;
        thirdByteLookupTable[i] = 12;
    }
}

const { indexOffsetByteWidth: absoluteOffsetByteWidth } = fragmentSettings;

type FragmentData = [absoluteOffset: number, relativeOffsets: Uint32Array];

export function decodeByteAlignedFragment(encodedFragment: Buffer, numIndexEntriesPerFragment = 4096): FragmentData {
    const absoluteOffset = convertUInt40LEToNumber(encodedFragment, 0);
    const tileSizes = new Uint32Array(numIndexEntriesPerFragment);

    let byteCounter = absoluteOffsetByteWidth;
    let previousRelativeOffset = 0;
    for (let i = 0; i < numIndexEntriesPerFragment; i++) {
        const tileSize =
            encodedFragment[byteCounter++] |
            (encodedFragment[byteCounter++] << 8) |
            (encodedFragment[byteCounter++] << 16);

        tileSizes[i] = previousRelativeOffset;
        previousRelativeOffset += tileSize;
    }

    return [absoluteOffset, tileSizes];
}

export function decodeBitAlignedFragment(
    encodedFragment: Buffer,
    numIndexEntriesPerFragment = 4096,
): [absoluteOffset: number, relativeOffsets: Uint32Array] {
    const absoluteOffset = convertUInt40LEToNumber(encodedFragment, 0);
    const tileSizes = new Uint32Array(numIndexEntriesPerFragment);

    let byteCounter = absoluteOffsetByteWidth;
    let previousRelativeOffset = 0;
    let partialStartByte = false;
    for (let i = 0; i < numIndexEntriesPerFragment; i++) {
        const tileSize = partialStartByte
            ? ((encodedFragment[byteCounter++] >> 4) & 0xf) |
              (encodedFragment[byteCounter++] << 4) |
              (encodedFragment[byteCounter++] << 12)
            : encodedFragment[byteCounter++] |
              (encodedFragment[byteCounter++] << 8) |
              ((encodedFragment[byteCounter] & 0xf) << 16);

        tileSizes[i] = previousRelativeOffset;
        previousRelativeOffset += tileSize;
        partialStartByte = !partialStartByte;
    }

    return [absoluteOffset, tileSizes];
}

export function decodeBitAlignedFragmentBranchless(
    encodedFragment: Buffer,
    numIndexEntriesPerFragment = 4096,
): [absoluteOffset: number, relativeOffsets: Uint32Array] {
    const absoluteOffset = convertUInt40LEToNumber(encodedFragment, 0);
    const tileSizes = new Uint32Array(numIndexEntriesPerFragment);

    let byteCounter = absoluteOffsetByteWidth;
    let previousRelativeOffset = 0;
    let partialStartByte = false;
    for (let i = 0; i < numIndexEntriesPerFragment; i++) {
        const tileSize =
            ((encodedFragment[byteCounter++] >> firstByteLookupTable[i]) & 0xf) |
            (encodedFragment[byteCounter++] << secondByteLookupTable[i]) |
            ((encodedFragment[byteCounter] & 0xf) << thirdByteLookupTable[i]);

        tileSizes[i] = previousRelativeOffset;
        previousRelativeOffset += tileSize;
        partialStartByte = !partialStartByte;
    }

    return [absoluteOffset, tileSizes];
}

export function decodeBitAlignedFragmentArrayBased(
    encodedFragment: Buffer,
    numIndexEntriesPerFragment = 4096,
): [absoluteOffset: number, relativeOffsets: number[]] {
    const absoluteOffset = convertUInt40LEToNumber(encodedFragment, 0);
    const tileSizes = new Array(numIndexEntriesPerFragment);

    let byteCounter = absoluteOffsetByteWidth;
    let previousRelativeOffset = 0;
    let partialStartByte = false;
    for (let i = 0; i < numIndexEntriesPerFragment; i++) {
        const tileSize = partialStartByte
            ? ((encodedFragment[byteCounter++] >> 4) & 0xf) |
              (encodedFragment[byteCounter++] << 4) |
              (encodedFragment[byteCounter++] << 12)
            : encodedFragment[byteCounter++] |
              (encodedFragment[byteCounter++] << 8) |
              ((encodedFragment[byteCounter] & 0xf) << 16);

        tileSizes[i] = previousRelativeOffset;
        previousRelativeOffset += tileSize;
        partialStartByte = !partialStartByte;
    }

    return [absoluteOffset, tileSizes];
}
