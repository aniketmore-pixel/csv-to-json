import express from "express";
import dotenv from "dotenv";
import { createReadStream } from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { parseCSVStream } from "./parser.js";
import { createDbClient, insertBatchToDB } from "./db.js";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// CSV file path and batch size
const CSV_PATH = process.env.CSV_PATH || path.join(__dirname, "../data/users.csv");
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "500", 10);

const app = express();
const port = process.env.PORT || 3000;

app.post("/upload-csv", async (req, res) => {
  console.log("Received POST request to upload CSV...");

  const client = createDbClient();

  // Connect to PostgreSQL
  try {
    await client.connect();
    console.log("Connected to PostgreSQL successfully.");
  } catch (err) {
    console.error("Database connection failed:", err.message);
    return res.status(500).json({ error: "Database connection failed" });
  }

  let batch = [];
  let total = 0;
  let recordCount = 0;

  try {
    const stream = createReadStream(CSV_PATH, { encoding: "utf8" });
    stream.on("error", (err) => console.error("Failed to read CSV file:", err.message));

    const parser = parseCSVStream(stream);

    // Process CSV records
    for await (const record of parser) {
      recordCount++;

      try {
        const firstName = (record["name.firstName"] ?? "").trim();
        const lastName = (record["name.lastName"] ?? "").trim();
        const age = Number(record.age);

        if (!firstName && !lastName) continue; // skip if name missing
        if (isNaN(age)) continue; // skip if age invalid

        const nameField = `${firstName} ${lastName}`.trim();

        const address = {
          line1: record["address.line1"] ?? null,
          line2: record["address.line2"] ?? null,
          city: record["address.city"] ?? null,
          state: record["address.state"] ?? null,
        };

        const additional_info = extractAdditionalInfo(record);

        batch.push({ name: nameField, age, address, additional_info });

        // Insert batch when size limit reached
        if (batch.length >= BATCH_SIZE) {
          await insertBatchToDB(client, batch);
          total += batch.length;
          batch = [];
        }
      } catch (err) {
        console.error(`Error processing record #${recordCount}:`, err.message);
      }
    }

    // Insert any remaining records
    if (batch.length > 0) {
      await insertBatchToDB(client, batch);
      total += batch.length;
    }

    console.log(`Successfully processed ${total} records.`);

    // --- Compute & print age distribution ---
    const dist = await computeAgeDistribution(client);
    printAgeDistribution(dist);

    res.json({ message: `Successfully inserted ${total} records.` });
  } catch (err) {
    console.error("CSV processing failed:", err.message);
    res.status(500).json({ error: "CSV processing failed" });
  } finally {
    await client.end().catch(() => {});
  }
});

// Extract extra fields as additional_info
function extractAdditionalInfo(record) {
  const clone = JSON.parse(JSON.stringify(record ?? {}));
  delete clone["name.firstName"];
  delete clone["name.lastName"];
  delete clone.age;
  delete clone["address.line1"];
  delete clone["address.line2"];
  delete clone["address.city"];
  delete clone["address.state"];
  return Object.keys(clone).length ? clone : null;
}

// Compute age distribution from DB
async function computeAgeDistribution(client) {
  try {
    const res = await client.query("SELECT age FROM public.users WHERE age IS NOT NULL");
    const ages = res.rows.map(r => Number(r.age)).filter(a => !isNaN(a));
    const total = ages.length;

    const buckets = { lt20: 0, between20and40: 0, between40and60: 0, gt60: 0 };
    for (const a of ages) {
      if (a < 20) buckets.lt20++;
      else if (a >= 20 && a < 40) buckets.between20and40++;
      else if (a >= 40 && a < 60) buckets.between40and60++;
      else buckets.gt60++;
    }

    const pct = total === 0
      ? { lt20: 0, between20and40: 0, between40and60: 0, gt60: 0 }
      : {
          lt20: Math.round((buckets.lt20 / total) * 100),
          between20and40: Math.round((buckets.between20and40 / total) * 100),
          between40and60: Math.round((buckets.between40and60 / total) * 100),
          gt60: Math.round((buckets.gt60 / total) * 100),
        };

    return { total, counts: buckets, percentages: pct };
  } catch (err) {
    console.error("Failed to compute age distribution:", err.message);
    return { total: 0, counts: {}, percentages: {} };
  }
}

// Print age distribution report
function printAgeDistribution(dist) {
  console.log("\nAge-Group % Distribution (calculated from DB):\n");
  console.log(`< 20\t\t${dist.percentages.lt20}%`);
  console.log(`20 to 40\t${dist.percentages.between20and40}%`);
  console.log(`40 to 60\t${dist.percentages.between40and60}%`);
  console.log(`> 60\t\t${dist.percentages.gt60}%`);
  console.log(`\nTotal users counted: ${dist.total}\n`);
}

// Start Express server
app.listen(port, () => console.log(`Server started on http://localhost:${port}`));
