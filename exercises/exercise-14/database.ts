import { readFile } from "fs";

type FieldQuery<FT> = { $eq: FT } | { $gt: FT } | { $lt: FT } | { $in: FT[] };

type QueryOption<T> = {
    $text?: string;
    $and?: Query<T>[];
    $or?: Query<T>[];
};

type Query<T extends {}> = { [K in keyof T]?: FieldQuery<T[K]> } & QueryOption<T>;

type Constraint<T> = (item: T) => boolean;

type Config<T> = {
    projection?: { [key in keyof T]?: 1 };
    sort?: { [key in keyof T]?: 1 };
};
export class Database<T> {
    protected rows: T[] = [];

    constructor(protected filename: string, protected fullTextSearchFieldNames: (keyof T)[]) {}

    async load(filename: string): Promise<T[]> {
        return new Promise((resolve, reject) => {
            readFile(filename, null, (err, data) => {
                if (err) reject(err);
                const lines = data
                    .toString()
                    .split("\n")
                    .filter((i) => i.startsWith("E"));
                const rows = lines.map((i) => JSON.parse(i.substring(1)));
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

    // TODO return type
    async find(query: Query<T>, config?: Config<T>): Promise<T[]> {
        if (this.rows.length === 0) this.rows = await this.load(this.filename);
        const constraint = this.genConstraint(query);
        let result = this.rows.filter(constraint);
        if (config) {
            if (config.projection) {
                // @ts-ignore
                result = result.map((item) => {
                    const _item: Partial<T> = {};
                    for (let i in config.projection) _item[i] = item[i];
                    return _item;
                });
            }
            if (config.sort) {
                for (let key in config.sort) {
                    result = result.sort((a, b) => {
                        // @ts-ignore
                        return (a[key] - b[key]) * config.sort[key]
                    })
                }
            }
        }
        return result;
    }
}
