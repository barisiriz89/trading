import { Firestore } from "@google-cloud/firestore";

const COLLECTION = process.env.FS_COLLECTION || "positions";
const DOC_ID = process.env.FS_DOC || "mainnet:BTCUSDT";
const DRY_RUN = (process.env.DRY_RUN || "true").toLowerCase() === "true";

const db = new Firestore();
const ref = db.collection(COLLECTION).doc(DOC_ID);

const snap = await ref.get();
if (!snap.exists) {
  console.log(`✅ Document not found: ${COLLECTION}/${DOC_ID} (nothing to delete)`);
  process.exit(0);
}

const data = snap.data() || {};
const cycles = Array.isArray(data.cycles) ? data.cycles : [];
const openCycles = cycles.filter((c) => c && c.status === "OPEN");
const coreQty = data?.core?.qty ?? 0;

console.log(`📌 Found: ${COLLECTION}/${DOC_ID}`);
console.log(`   core.qty: ${coreQty}`);
console.log(`   cycles total: ${cycles.length}`);
console.log(`   OPEN cycles: ${openCycles.length}`);
console.log(`   pausedUntilMs: ${data.pausedUntilMs ?? 0}`);
console.log(`   lastActionMs: ${data.lastActionMs ?? 0}`);

if (DRY_RUN) {
  console.log("🟡 DRY_RUN=true -> NOT deleting. Run with DRY_RUN=false to delete.");
  process.exit(0);
}

await ref.delete();
console.log(`🗑️ Deleted: ${COLLECTION}/${DOC_ID}`);
