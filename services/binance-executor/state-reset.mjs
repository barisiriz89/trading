import { Firestore } from "@google-cloud/firestore";
const [,, envName, symbol] = process.argv;
if (!envName || !symbol) {
  console.error("usage: node state-reset.mjs mainnet BTCUSDT");
  process.exit(1);
}
const db = new Firestore({ projectId: process.env.GOOGLE_CLOUD_PROJECT });
const col = process.env.FIRESTORE_COLLECTION || "positions";
const id = `${envName}:${symbol}`;
await db.collection(col).doc(id).set({
  env: envName,
  symbol,
  qty: 0,
  avgPrice: 0,
  costUSDT: 0,
  feesUSDT: 0,
  dcaCount: 0,
  lastDcaMs: 0,
  pausedUntilMs: 0,
  tpPrice: 0,
  tpOrderId: null
}, { merge: true });
console.log("reset ok", { col, id });
