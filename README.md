# CSV to PostgreSQL Uploader

A Node.js API to parse a CSV file and upload its contents to a PostgreSQL database. Supports batch insertion and handles nested CSV headers and JSON fields.

---

## Features

- Parses CSV files with nested headers like `name.firstName`, `address.city`.
- Converts rows into JSON objects with `name`, `age`, `address`, and `additional_info`.
- Batch insertion to PostgreSQL for performance.
- Fully asynchronous streaming to handle large CSV files.
- Simple POST endpoint to trigger the upload.

---

## Prerequisites

- Node.js >= 18
- PostgreSQL database
- npm

---

## Setup

1. **Clone the repository**  
   ```bash
   git clone <repo-url>
   cd csv-to-json-uploader
   ```

2. **Install dependencies**  
   ```bash
   npm install
   ```

3. **Create a `.env` file** in the root:  
   ```env
   PORT=3000
   DB_HOST=localhost
   DB_PORT=5432
   DB_USER=your_db_user
   DB_PASSWORD=your_db_password
   DB_NAME=your_db_name
   CSV_PATH=./data/users.csv
   BATCH_SIZE=500
   ```

4. **Create the `users` table** in PostgreSQL:  
   ```sql
   CREATE TABLE public.users (
     id serial PRIMARY KEY,
     name varchar NOT NULL,
     age int NOT NULL,
     address jsonb NULL,
     additional_info jsonb NULL
   );
   ```

---

## Usage

1. **Start the server**  
   ```bash
   npm start
   ```

2. **Send a POST request to upload the CSV**  
   - Endpoint: `POST /upload-csv`  
   - No body required — the server will read the CSV from the path specified in `.env`.

3. **Response**  
   ```json
   {
     "message": "Successfully inserted 4 records."
   }
   ```

---

## Sample CSV

```csv
name.firstName,name.lastName,age,address.line1,address.line2,address.city,address.state,gender,employment.title
Rohit,Prasad,35,"A-563 Rakshak Society","New Pune Road",Pune,Maharashtra,male,"Engineer"
Jane,O'Brien,28,"123 Main St","Apt 4B","Mumbai","Maharashtra",female,"Product Manager"
Bob,Smith,18,"44 Lake Rd",,"Bengaluru","Karnataka",male,"Intern"
Jake,Smith,18,"44 Lake Rd",,"Bengaluru","Karnataka",male,"Intern"
```

---

## Project Structure

```
csv-to-json-uploader/
│
├─ src/
│  ├─ index.js        # Main server and CSV processing
│  ├─ db.js           # PostgreSQL connection & batch insertion
│  └─ parser.js       # CSV parsing logic
│
├─ data/
│  └─ users.csv       # Sample CSV file
│
├─ .env               # Environment variables
├─ package.json
└─ README.md
```

---

## Notes

- CSV headers must match the nested key format used in the code (`name.firstName`, `address.city`, etc.).
- Additional columns in the CSV are stored in `additional_info` as JSON.
- Handles quotes, escaped quotes, and incomplete lines.
- Batch size can be adjusted via the `BATCH_SIZE` environment variable.

---

