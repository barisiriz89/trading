import { Firestore } from "@google-cloud/firestore";

const db = new Firestore();
const COLLECTION = process.env.FIRESTORE_COLLECTION || "positions";

const env = process.argv[2] || "mainnet";
const symbol = process.argv[3] || "BTCUSDT";
const id = `${env}:${symbol}`;

const ref = db.collection(COLLECTION).doc(id);
const snap = await ref.get();

if (!snap.exists) {
  console.log("NOT_FOUND", { collection: COLLECTION, id });
  process.exit(0);
}

console.log(JSON.stringify({ collection: COLLECTION, id, ...snap.data() }, null, 2));
