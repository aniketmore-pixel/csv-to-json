import { Client } from "pg";
import dotenv from "dotenv";

dotenv.config();

// Create a new PostgreSQL client
export function createDbClient() {
  return new Client({
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT),
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
  });
}

// Insert a batch of records into the users table
export async function insertBatchToDB(client, batch) {
  if (!batch.length) return; // nothing to insert

  // Build dynamic placeholders for parameterized query
  const query =
    "INSERT INTO public.users(name, age, address, additional_info) VALUES " +
    batch
      .map(
        (_, i) =>
          `($${i * 4 + 1}, $${i * 4 + 2}, $${i * 4 + 3}::jsonb, $${i * 4 + 4}::jsonb)`
      )
      .join(", ");

  // Flatten batch values for query
  const values = batch.flatMap((r) => [
    r.name,
    r.age,
    JSON.stringify(r.address),
    JSON.stringify(r.additional_info),
  ]);

  await client.query(query, values); // execute insertion
}
