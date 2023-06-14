export declare function buildInList(values: Array<string>, startIdx?: number): Array<string>;
export interface Query {
    text: string;
    values: Array<any>;
}
export declare function buildMultiInsert<T>(stmt: string, template: string, data: Array<any>, mapping: (data: T) => Array<any>): Query;
//# sourceMappingURL=utils.d.ts.map