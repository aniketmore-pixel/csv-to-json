import { Readable } from "stream";

// Async generator: parses CSV stream line by line and yields row objects
export async function* parseCSVStream(stream) {
  let headers = [];
  let leftover = "";

  for await (const chunk of stream) {
    const data = leftover + chunk;
    const lines = data.split(/\r?\n/); // split into lines
    leftover = lines.pop(); // keep last line if incomplete

    for (const line of lines) {
      const row = parseCSVLine(line);

      if (headers.length === 0) {
        headers = row; // first line = headers
        continue;
      }

      const record = {};
      for (let i = 0; i < headers.length; i++) {
        record[headers[i]] = row[i] ?? ""; // map values to headers
      }
      yield record;
    }
  }

  // handle leftover line if any
  if (leftover) {
    const row = parseCSVLine(leftover);
    if (row.length && headers.length) {
      const record = {};
      for (let i = 0; i < headers.length; i++) {
        record[headers[i]] = row[i] ?? "";
      }
      yield record;
    }
  }
}

// Parses a single CSV line into an array of values, handling quotes
function parseCSVLine(line) {
  const result = [];
  let curr = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];

    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        curr += '"'; // escaped quote
        i++;
      } else {
        inQuotes = !inQuotes; // toggle quote state
      }
    } else if (char === "," && !inQuotes) {
      result.push(curr); // end of field
      curr = "";
    } else {
      curr += char; // append character
    }
  }

  result.push(curr); // push last field
  return result;
}
