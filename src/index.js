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

    stream.on("error", (err) => {
      console.error("Failed to read CSV file:", err.message);
    });

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

// Start Express server
app.listen(port, () => {
  console.log(`Server started on http://localhost:${port}`);
});
