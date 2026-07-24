import { readFileSync } from "node:fs";

import { parse as parseYaml } from "yaml";

// Loads ops/instruments.yml into the two lookup tables the gateway uses on the
// hot path:
//   1. (exchange, symbol) -> canonical_id   (every consumed BBO routed here)
//   2. canonical_id       -> CanonicalInstrument (NBBO state + topic creation)
// See docs/DESIGN_nbbo.md "Canonical map in repo YAML, resolved gateway-side".

export interface VenueRef {
  exchange: string;
  symbol: string;
}

export interface CanonicalInstrument {
  canonical_id: string;
  base: string;
  quote: string;
  venues: VenueRef[];
}

// Shape of the parsed YAML (validated, then mapped to CanonicalInstrument).
interface InstrumentsFile {
  instruments: Record<string, { base: string; quote: string; venues: VenueRef[] }>;
}

export class CanonicalMap {
  private readonly byCanonicalId = new Map<string, CanonicalInstrument>();
  private readonly byVenueKey = new Map<string, CanonicalInstrument>();

  constructor(instruments: CanonicalInstrument[]) {
    for (const inst of instruments) {
      this.byCanonicalId.set(inst.canonical_id, inst);
      for (const v of inst.venues) {
        const key = venueKey(v.exchange, v.symbol);
        const existing = this.byVenueKey.get(key);
        if (existing) {
          throw new Error(
            `instruments.yml: venue ${key} mapped to both ` +
              `${existing.canonical_id} and ${inst.canonical_id}`,
          );
        }
        this.byVenueKey.set(key, inst);
      }
    }
  }

  // Hot-path lookup. Returns undefined for venues not declared in the map -
  // gateway treats this as "BBO from an unmapped venue, skip NBBO routing"
  // rather than an error (allows incrementally adding venues).
  lookup(exchange: string, symbol: string): CanonicalInstrument | undefined {
    return this.byVenueKey.get(venueKey(exchange, symbol));
  }

  get(canonical_id: string): CanonicalInstrument | undefined {
    return this.byCanonicalId.get(canonical_id);
  }

  all(): CanonicalInstrument[] {
    return [...this.byCanonicalId.values()];
  }
}

function venueKey(exchange: string, symbol: string): string {
  return `${exchange}|${symbol}`;
}

export function loadCanonicalMap(path: string): CanonicalMap {
  const raw = readFileSync(path, "utf8");
  const parsed = parseYaml(raw) as InstrumentsFile | null;
  if (!parsed || typeof parsed !== "object" || !parsed.instruments) {
    throw new Error(`${path}: missing top-level 'instruments' key`);
  }
  const instruments: CanonicalInstrument[] = [];
  for (const [canonical_id, entry] of Object.entries(parsed.instruments)) {
    if (!entry.base || !entry.quote || !Array.isArray(entry.venues)) {
      throw new Error(`${path}: ${canonical_id} missing base/quote/venues`);
    }
    for (const v of entry.venues) {
      if (!v.exchange || !v.symbol) {
        throw new Error(`${path}: ${canonical_id} has a venue entry missing exchange/symbol`);
      }
    }
    instruments.push({ canonical_id, base: entry.base, quote: entry.quote, venues: entry.venues });
  }
  return new CanonicalMap(instruments);
}
