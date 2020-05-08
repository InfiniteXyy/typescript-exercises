import { readFile, readFileSync, writeFile, writeFileSync } from "fs";

type FieldQuery<FT> = { $eq: FT } | { $gt: FT } | { $lt: FT } | { $in: FT[] };
type QueryOption<T> = {
    $text?: string;
    $and?: Query<T>[];
    $or?: Query<T>[];
};
type Query<T extends {}> = { [K in keyof T]?: FieldQuery<T[K]> } & QueryOption<T>;
type Constraint<T> = (item: T) => boolean;
type SortOption<T> = { [key in keyof T]?: number };
type ProjectionOption<T> = { [key in keyof T]?: 1 };
type Config<T> = {
    projection?: ProjectionOption<T>;
    sort?: SortOption<T>;
};
export class Database<T> {
    protected rows: T[] = [];
    protected rawData: string[] = [];
    protected lock = false;

    constructor(protected filename: string, protected fullTextSearchFieldNames: (keyof T)[]) {}

    async load(filename: string): Promise<T[]> {
        return new Promise((resolve, reject) => {
            readFile(filename, null, (err, data) => {
                if (err) reject(err);
                this.rawData = data
                    .toString()
                    .split(/[\n\r]/)
                    .filter((i) => !!i.trim());
                const lines = data
                    .toString()
                    .split(/[\n\r]/)
                    .filter((i) => i.startsWith("E"));
                const rows = lines.map((i) => JSON.parse(i.substring(1)));
                this.rows = rows;
                resolve(rows);
            });
        });
    }

    isOption(key: keyof Query<T>): key is keyof QueryOption<T> {
        return ["$text", "$and", "$or"].includes(key as string);
    }

    genConstraint(query: Query<T>): Constraint<T> {
        const constraints: Constraint<T>[] = [];
        if ("$and" in query) {
            constraints.push((item) => {
                const innerConstraints = query.$and!.map((innerQuery) => this.genConstraint(innerQuery));
                return innerConstraints.every((fn) => fn(item));
            });
        }
        if ("$or" in query) {
            constraints.push((item) => {
                const innerConstraints = query.$or!.map((innerQuery) => this.genConstraint(innerQuery));
                return innerConstraints.some((fn) => fn(item));
            });
        }
        if ("$text" in query) {
            const value = query.$text!.toLowerCase();
            constraints.push((item) => {
                for (let field of this.fullTextSearchFieldNames) {
                    if (
                        String(item[field])
                            .split(" ")
                            .some((i: any) => i.toLowerCase() === value)
                    ) {
                        return true;
                    }
                }
                return false;
            });
        }
        (Object.keys(query) as (keyof Query<T>)[]).forEach((key) => {
            if (!this.isOption(key)) {
                let constraint: Constraint<T> = () => true;
                const compareValue = query[key];
                if ("$eq" in compareValue) constraint = (item: T) => item[key] === compareValue.$eq;
                if ("$gt" in compareValue) constraint = (item: T) => item[key] > compareValue.$gt;
                if ("$lt" in compareValue) constraint = (item: T) => item[key] < compareValue.$lt;
                if ("$in" in compareValue)
                    constraint = (item: T) => {
                        for (const val of compareValue.$in) if (item[key] === val) return true;
                        return false;
                    };
                constraints.push(constraint);
            }
        });
        return (item) => constraints.every((fn) => fn(item));
    }

    async find(query: Query<T>): Promise<T[]>;
    async find(query: Query<T>, config: { sort: SortOption<T> }): Promise<T[]>;
    async find<PT extends ProjectionOption<T>>(
        query: Query<T>,
        config: { projection: PT; sort?: SortOption<T> }
    ): Promise<{ [key in Extract<keyof T, keyof PT>]: T[key] }[]>;

    async find(query: Query<T>, config?: Config<T>): Promise<T[]> {
        await this.load(this.filename);
        const constraint = this.genConstraint(query);
        let result: T[] = this.rows.filter(constraint);
        if (config?.sort) {
            (Object.keys(config.sort) as (keyof SortOption<T>)[]).forEach((key) => {
                result.sort((a, b) => {
                    if (a[key] < b[key]) return -config.sort![key];
                    if (a[key] > b[key]) return +config.sort![key];
                    return 0;
                });
            });
        }
        if (config?.projection) {
            const { projection } = config;
            return result.map((item) => {
                const _item: Partial<T> = {};
                (Object.keys(projection) as (keyof T)[]).forEach((i) => {
                    _item[i] = item[i];
                });
                return _item;
            }) as T[];
        }
        return result;
    }

    async delete(query: Query<T>) {
        while (this.lock) {}
        this.lock = true;
        const shouldDelete = this.genConstraint(query);
        const rawData = readFileSync(this.filename)
            .toString()
            .split(/[\n\r]/)
            .filter((i) => !!i.trim());
        return new Promise((resolve, reject) => {
            writeFileSync(
                this.filename,
                rawData
                    .map((i) => {
                        if (i.startsWith("D")) return i;
                        const obj = JSON.parse(i.substring(1));
                        if (shouldDelete(obj)) {
                            return i.replace(/^E/, "D");
                        }
                        return i;
                    })
                    .join("\n")
            );
            this.lock = false;
            resolve();
        });
    }

    async insert(row: T) {
        return new Promise(async (resolve, reject) => {
            while (this.lock) {}
            this.lock = true;
            const file = readFileSync(this.filename).toString();
            writeFileSync(this.filename, file + "\nE" + JSON.stringify(row));
            this.lock = false;
            resolve();
        });
    }
}
