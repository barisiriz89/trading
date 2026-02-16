export function parseAllowlist(value) {
  if (!value) return [];
  return value.split(",").map(s => s.trim()).filter(Boolean);
}

export function isSymbolAllowed(symbol, allowlistValue) {
  const list = Array.isArray(allowlistValue) ? allowlistValue : parseAllowlist(allowlistValue);
  if (!list.length) return true; // empty => allow all (istersen sonra strict yaparız)
  return list.includes(symbol);
}
