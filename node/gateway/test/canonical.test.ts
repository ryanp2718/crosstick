import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

import { describe, expect, it } from "vitest";

import { CanonicalMap, loadCanonicalMap } from "../src/canonical.js";

function writeTempYaml(contents: string): string {
  const dir = mkdtempSync(path.join(tmpdir(), "canonical-test-"));
  const file = path.join(dir, "instruments.yml");
  writeFileSync(file, contents, "utf8");
  return file;
}

describe("CanonicalMap", () => {
  it("looks up canonical by (exchange, symbol)", () => {
    const m = new CanonicalMap([
      {
        canonical_id: "BTC-USD",
        base: "BTC",
        quote: "USD",
        venues: [
          { exchange: "coinbase", symbol: "BTC-USD" },
          { exchange: "kraken", symbol: "BTC/USD" },
        ],
      },
    ]);
    expect(m.lookup("coinbase", "BTC-USD")?.canonical_id).toBe("BTC-USD");
    expect(m.lookup("kraken", "BTC/USD")?.canonical_id).toBe("BTC-USD");
    expect(m.lookup("binance", "BTCUSDT")).toBeUndefined();
  });

  it("throws when one venue maps to two canonicals", () => {
    expect(
      () =>
        new CanonicalMap([
          {
            canonical_id: "A",
            base: "A",
            quote: "B",
            venues: [{ exchange: "x", symbol: "y" }],
          },
          {
            canonical_id: "C",
            base: "C",
            quote: "D",
            venues: [{ exchange: "x", symbol: "y" }],
          },
        ]),
    ).toThrow(/x\|y mapped to both/);
  });

  it("get() / all() expose canonicals", () => {
    const m = new CanonicalMap([
      { canonical_id: "X", base: "X", quote: "Y", venues: [] },
    ]);
    expect(m.get("X")?.base).toBe("X");
    expect(m.all()).toHaveLength(1);
  });
});

describe("loadCanonicalMap", () => {
  it("parses a well-formed YAML file", () => {
    const file = writeTempYaml(`
instruments:
  BTC-USD:
    base: BTC
    quote: USD
    venues:
      - { exchange: coinbase, symbol: BTC-USD }
`);
    const m = loadCanonicalMap(file);
    expect(m.lookup("coinbase", "BTC-USD")?.base).toBe("BTC");
  });

  it("rejects files missing 'instruments' key", () => {
    const file = writeTempYaml(`foo: bar\n`);
    expect(() => loadCanonicalMap(file)).toThrow(/missing top-level 'instruments'/);
  });

  it("rejects an instrument missing base/quote/venues", () => {
    const file = writeTempYaml(`
instruments:
  BAD:
    base: X
    venues: []
`);
    expect(() => loadCanonicalMap(file)).toThrow(/missing base\/quote\/venues/);
  });

  it("rejects a venue entry missing exchange or symbol", () => {
    const file = writeTempYaml(`
instruments:
  X-Y:
    base: X
    quote: Y
    venues:
      - { exchange: foo }
`);
    expect(() => loadCanonicalMap(file)).toThrow(/missing exchange\/symbol/);
  });
});
