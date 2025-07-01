import requests
import json
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database connection parameters
db_params = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": "lsd",
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# === Step 1: Elasticsearch SCROLL Query ===
es_url = "http://localhost:9200/pdf_documents_clean/_search?scroll=2m"
scroll_url = "http://localhost:9200/_search/scroll"
headers = {"Content-Type": "application/json"}

keywords = [
    "Builder", "Developer", "Realty", "Housing",
    "Society", "Construction", "Home", "Real Estate"
]

es_query = {
    "size": 1000,
    "query": {
        "bool": {
            "should": [
                {"match": {"pymupdf4llm": {"query": word, "fuzziness": "AUTO"}}}
                for word in keywords
            ]
        }
    },
    "_source": ["pdf_id"]
}

print("🔍 Querying Elasticsearch with scroll API...")
try:
    response = requests.post(es_url, headers=headers, data=json.dumps(es_query))
    response.raise_for_status()
    result = response.json()

    scroll_id = result["_scroll_id"]
    hits = result["hits"]["hits"]
    pdf_ids = set()

    while hits:
        for hit in hits:
            pdf_id = hit["_source"].get("pdf_id")
            if pdf_id is not None:
                pdf_ids.add(int(pdf_id))

        scroll_payload = {"scroll": "2m", "scroll_id": scroll_id}
        scroll_response = requests.post(scroll_url, headers=headers, data=json.dumps(scroll_payload))
        scroll_response.raise_for_status()
        result = scroll_response.json()
        scroll_id = result.get("_scroll_id")
        hits = result["hits"]["hits"]

    pdf_ids = list(pdf_ids)
    print(f"✅ Found {len(pdf_ids)} matching pdf_id(s).")

except Exception as e:
    print(f"❌ Elasticsearch scroll query failed: {e}")
    pdf_ids = []

# === Step 2: Query PostgreSQL ===
if pdf_ids:
    try:
        print("📦 Connecting to PostgreSQL...")
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        sql = """
        SELECT
            m.matter_id,
            m.filing_no,
            m.case_status,
            m.case_typology,
            m.filing_date,
            m.disposal_date,
            m.court_id,
            m.cnr,
            m.petitioners,
            m.respondents,
            h.order_pdf AS pdf_id,
            h.hearing_date,
            pt.pymupdf4llm
        FROM
            lsd.hearings h
        JOIN
            lsd.matters m ON h.matter_id = m.matter_id
        JOIN 
            public.pdf_text pt ON h.order_pdf = pt.pdf_id
        WHERE
            h.order_pdf = ANY(%s)
            AND m.court_id IN (1, 14)
            AND m.filing_date BETWEEN '2021-01-01' AND '2024-12-31'
        """

        print("📄 Running SQL query...")
        logger.info("Executing SQL query...")
        cursor.execute(sql, (pdf_ids,))
        rows = cursor.fetchall()

        if rows:
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)

            # Clean the pymupdf4llm column
            df["pymupdf4llm"] = (
                df["pymupdf4llm"]
                .astype(str)
                .str.replace(r"[\r\n]+", " ", regex=True)
                .str.replace(r"\s{2,}", " ", regex=True)
                .str.replace('"', "'", regex=False)
                .str.replace(r"\bWITH\b", "", regex=True)
                .str.replace(r"[-]{5,}", "", regex=True)
                .str.strip()
            )

            # Save CSVs
            df_main = df.drop(columns=["pymupdf4llm"])
            df_text = df[["matter_id", "pdf_id", "pymupdf4llm"]]

            df_main.to_csv("real_estate_matters_main.csv", index=False)
            df_text.to_csv("real_estate_matters_text.csv", index=False)

            print("✅ Saved:")
            print("  - real_estate_matters_main.csv (without full text)")
            print("  - real_estate_matters_text.csv (only matter_id, pdf_id, and cleaned text)")

        else:
            print("⚠️ No matching records found in PostgreSQL.")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ PostgreSQL query failed: {e}")
        logger.error(f"PostgreSQL error: {e}")
        if "cursor" in locals():
            cursor.close()
        if "conn" in locals():
            conn.close()
else:
    print("⚠️ No matching pdf_ids found in Elasticsearch.")
