// Exact ordering for non-negative decimal strings (prices and sizes), no floats.
//
// Values arrive as strings ("45285.2", "0.05005"). Parsing to a JS number would
// risk precision loss on long fractions, so they are compared by raw digits.
// Used for book-level ordering and NBBO winner + size-tie-break selection. A tiny
// size can arrive in scientific notation ("1E-8"), so exponent tokens are first
// expanded to plain digits - the raw-digit comparison alone would mis-sort them.
export function cmpDecimal(a: string, b: string): number {
  a = toPlainDecimal(a);
  b = toPlainDecimal(b);
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

// Expand a scientific-notation decimal ("1E-8", "1.5e+3") to plain digits
// ("0.00000001", "1500"); non-exponent tokens return unchanged. Non-negative only.
function toPlainDecimal(s: string): string {
  let e = s.indexOf("e");
  if (e === -1) e = s.indexOf("E");
  if (e === -1) return s;
  const exp = parseInt(s.slice(e + 1), 10);
  const mant = s.slice(0, e);
  const dot = mant.indexOf(".");
  const intPart = dot === -1 ? mant : mant.slice(0, dot);
  const digits = dot === -1 ? mant : intPart + mant.slice(dot + 1);
  const point = intPart.length + exp; // decimal point index into the digit run
  if (point <= 0) return "0." + "0".repeat(-point) + digits;
  if (point >= digits.length) return digits + "0".repeat(point - digits.length);
  return digits.slice(0, point) + "." + digits.slice(point);
}

// A level with size numerically equal to zero is a removal. Exchanges spell it
// "0", "0.0", "0.00000000", etc.; Number() collapses all of those to 0 while
// keeping a genuine tiny size (1e-8) non-zero.
export function isZeroSize(size: string): boolean {
  return Number(size) === 0;
}
