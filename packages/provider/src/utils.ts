export function convertUInt40LEToNumber(buffer: ArrayBuffer, offset: number): number {
    const dataView = new DataView(buffer);
    return dataView.getUint32(offset + 1, true) * 0x100 + dataView.getUint8(offset);
}

export function convertUInt24LEToNumber(buffer: ArrayBuffer, offset: number): number {
    //TODO: refactor
    const dataView = new DataView(buffer);

    const off0 = dataView.getUint8(offset);
    const off1 = dataView.getUint8(offset + 1);
    const off2 = dataView.getUint8(offset + 2);

    const off0S = ("00" + off0.toString(16)).slice(-2);
    const off1S = ("00" + off1.toString(16)).slice(-2);
    const off2S = ("00" + off2.toString(16)).slice(-2);

    const offLE = off2S + off1S + off0S;

    return parseInt(offLE, 16);
}

export class Optional<T> {
    private constructor(private readonly _value: T) {}

    static of<T>(value: T): Optional<T> {
        return new Optional<T>(value);
    }

    static empty<T>(): Optional<T> {
        return new Optional<T>(null);
    }

    isPresent(): boolean {
        return this._value !== null;
    }

    get(): T {
        return this._value;
    }
}
