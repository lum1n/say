import { readFile } from "node:fs/promises";

import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";

const ajv = new Ajv2020({
  allErrors: true,
  strict: true,
  strictRequired: false,
});
addFormats(ajv);

for (const path of [
  "../../../protocol/worker-envelope.schema.json",
  "../../../protocol/client-envelope.schema.json",
]) {
  const schema = JSON.parse(await readFile(new URL(path, import.meta.url), "utf8"));
  ajv.compile(schema);
  console.log(`schema ${path} is valid`);
}
