declare module 'stats' {
    type Comparator<T> = (a: T, b: T) => number
    type GetIndexFunction = <T>(input: T[], comparator: Comparator<T>) => number
    type GetElementFunction = <T>(input: T[], comparator: Comparator<T>) => T | null
    export const getMaxIndex: GetIndexFunction
    export const getMaxElement: GetElementFunction
    export const getMinIndex: GetIndexFunction
    export const getMinElement: GetElementFunction
    export const getMedianIndex: GetIndexFunction
    export const getMedianElement: GetElementFunction
    export function getAverageValue<T, V>(input: T[], getValue: (item: T) => V): V;
}
