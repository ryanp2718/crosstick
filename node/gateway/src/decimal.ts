// Exact ordering for non-negative decimal *price* strings, without floats.
//
// Prices arrive as strings ("45285.2", "0.05005"). Parsing to a JS number would
// risk precision loss on long fractions, so levels are sorted by comparing the
// raw digits. Sizes are never compared — only stored and emitted verbatim — so
// they never go through here.
export function cmpDecimal(a: string, b: string): number {
  const dotA = a.indexOf(".");
  const dotB = b.indexOf(".");
  const intA = (dotA === -1 ? a : a.slice(0, dotA)).replace(/^0+/, "");
  const intB = (dotB === -1 ? b : b.slice(0, dotB)).replace(/^0+/, "");
  if (intA.length !== intB.length) return intA.length - intB.length;
  if (intA !== intB) return intA < intB ? -1 : 1;

  const fracA = dotA === -1 ? "" : a.slice(dotA + 1);
  const fracB = dotB === -1 ? "" : b.slice(dotB + 1);
  const n = Math.max(fracA.length, fracB.length);
  const padA = fracA.padEnd(n, "0");
  const padB = fracB.padEnd(n, "0");
  return padA === padB ? 0 : padA < padB ? -1 : 1;
}

// A level with size numerically equal to zero is a removal. Exchanges spell it
// "0", "0.0", "0.00000000", etc.; Number() collapses all of those to 0 while
// keeping a genuine tiny size (1e-8) non-zero.
export function isZeroSize(size: string): boolean {
  return Number(size) === 0;
}
