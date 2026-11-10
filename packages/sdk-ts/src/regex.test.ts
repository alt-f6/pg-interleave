import { describe, it, expect } from "vitest";
import { regexToRustPattern } from "./index";

describe("regexToRustPattern semantics", () => {
    it("enables (?s) dotAll by default fol plain RegExp", () => {
        const p = regexToRustPattern(/SELECT \* FROM jobs/);
        expect(p).toBe("(?s)SELECT \\* FROM jobs");
    });

    it("enables (?is) for case-insensitive RegExp", () => {
        const p = regexToRustPattern(/select \* from jobs/i);
        expect(p).toBe("(?is)select \\* from jobs");
    });

    it("preserves explicit Rust flags if passed as string starting with (?", () => {
        const p = regexToRustPattern("(?m)^SELECT");
        expect(p).toBe("(?m)^SELECT");
    });

    it("adds (?sa) to plain string patterns without flags", () => {
        const p = regexToRustPattern("UPDATE pgboss\\.job*SET state = 'completed'");
        expect(p).toBe("(?s)UPDATE pgboss\\.job*SET state = 'completed'");
    });

    it("matches multiline SQL across newlines using dotALL", () => {
        const rustPattern = regexToRustPattern(/WITH.*UPDATE.*SET state = 'completed'/i);
        expect(rustPattern).toBe("(?is)WITH.*UPDATE.*SET state = 'completed'");
    });
});