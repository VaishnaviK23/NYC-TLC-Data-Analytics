import os, json, time, re, random
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "anthropic.claude-3-5-sonnet-20240620-v1:0")
ATHENA_WORKGROUP = os.environ["ATHENA_WORKGROUP"]
ATHENA_DATABASE  = os.environ["ATHENA_DATABASE"]
ATHENA_OUTPUT_S3 = os.environ["ATHENA_OUTPUT_S3"]
MAX_ROWS         = int(os.environ.get("MAX_ROWS", "100"))
ALLOWED_SCHEMAS  = set([s.strip() for s in os.environ.get("ALLOWED_SCHEMAS","nyc_taxi").split(",")])

# ---------- CLIENTS (with retries) ----------
bedrock = boto3.client(
    "bedrock-runtime",
    config=Config(
        retries={"max_attempts": 10, "mode": "adaptive"},
        read_timeout=30, connect_timeout=5,
    ),
)
athena = boto3.client("athena")


# ---- Simple schema spec for prompting (update if you add more views)
SCHEMA_SPEC = """
You can query these Athena tables in database nyc_taxi:

Table nyc_taxi.yellow_curated columns:
  vendorid int, tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp,
  passenger_count int, trip_distance double, ratecodeid int, store_and_fwd_flag string,
  pulocationid int, dolocationid int, payment_type int, fare_amount double, extra double,
  mta_tax double, tip_amount double, tolls_amount double, improvement_surcharge double,
  total_amount double, congestion_surcharge double, airport_fee double,
  year int, month int

Table nyc_taxi.taxi_zone_lookup columns:
  LocationID int, Borough string, Zone string, service_zone string

View nyc_taxi.v_trips_borough_hour columns:
  pickup_hour timestamp, pickup_borough string, dropoff_borough string,
  trip_distance double, passenger_count int, fare_amount double, tip_amount double, total_amount double
"""

SYSTEM_MSG = """You are a senior analytics engineer that writes Athena SQL for NYC taxi data.
Rules:
- Output ONLY a SQL query. No Markdown, no backticks, no comments.
- READ-ONLY: SELECT queries only. No INSERT/UPDATE/DELETE/CREATE/DROP.
- Add a LIMIT if user didn't provide one (default {max_rows}).
- Use the provided schema. If user asks for boroughs, join zone lookup when needed.
- Prefer partition filters (year, month) when time periods are specified.
""".format(max_rows=MAX_ROWS)

FEW_SHOTS = [
    {
        "user": "Which pickup borough had the highest average tip percentage on Saturday nights in 2024?",
        "sql": """
SELECT zpu.Borough AS pickup_borough,
       AVG(tip_amount / NULLIF(fare_amount,0)) AS avg_tip_pct
FROM nyc_taxi.yellow_curated y
LEFT JOIN nyc_taxi.taxi_zone_lookup zpu ON y.pulocationid = zpu.LocationID
WHERE y.year = 2024
  AND extract(dow FROM y.tpep_pickup_datetime) = 6   -- Sat
  AND extract(hour FROM y.tpep_pickup_datetime) BETWEEN 20 AND 23
  AND fare_amount > 0
GROUP BY zpu.Borough
ORDER BY avg_tip_pct DESC
LIMIT 50
""".strip()
    },
    {
        "user": "Show trips and total revenue by hour for Manhattan pickups in July 2024.",
        "sql": """
SELECT date_trunc('hour', y.tpep_pickup_datetime) AS hr,
       COUNT(*) AS trips,
       SUM(total_amount) AS revenue
FROM nyc_taxi.yellow_curated y
LEFT JOIN nyc_taxi.taxi_zone_lookup zpu ON y.pulocationid = zpu.LocationID
WHERE y.year = 2024 AND y.month = 7
  AND zpu.Borough = 'Manhattan'
GROUP BY 1
ORDER BY 1
LIMIT 1000
""".strip()
    }
]

# ---------- HELPERS ----------
def invoke_bedrock_with_retry(payload: dict, max_attempts=6):
    attempt = 0
    while True:
        try:
            resp = bedrock.invoke_model(modelId=BEDROCK_MODEL_ID, body=json.dumps(payload))
            return json.loads(resp["body"].read())
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("ThrottlingException", "TooManyRequestsException") and attempt < max_attempts - 1:
                backoff = min(8.0, (2 ** attempt) * 0.5) + random.uniform(0, 0.3)
                time.sleep(backoff)
                attempt += 1
                continue
            raise

def call_bedrock_for_sql(question: str) -> str:
    prompt = (
        "You will write a single Athena SQL query.\n"
        "Schema:\n" + SCHEMA_SPEC + "\n\n"
        "User question:\n" + question + "\n\n"
        "Return only SQL:"
    )

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 300,  # keep small to reduce throttling
        "system": [{"type": "text", "text": SYSTEM_MSG}],
        "messages": [
            {"role": "user",      "content": [{"type": "text", "text": FEW_SHOTS[0]["user"]}]},
            {"role": "assistant", "content": [{"type": "text", "text": FEW_SHOTS[0]["sql"]}]},
            {"role": "user",      "content": [{"type": "text", "text": FEW_SHOTS[1]["user"]}]},
            {"role": "assistant", "content": [{"type": "text", "text": FEW_SHOTS[1]["sql"]}]},
            {"role": "user",      "content": [{"type": "text", "text": prompt}]},
        ],
    }

    out = invoke_bedrock_with_retry(body)
    text = "".join(
        b.get("text", "") for b in out.get("content", []) if b.get("type") == "text"
    ).strip()
    return text.strip().strip("```").strip()

# --- Guardrails: simple sanitizer to block non-SELECT and schema hopping
SELECT_RE = re.compile(r"^\s*select\b", re.IGNORECASE | re.DOTALL)

def sanitize_sql(sql: str) -> str:
    sql = sql.strip()

    # 1) Only SELECTs (no multiple statements)
    if not re.match(r"^\s*select\b", sql, re.IGNORECASE | re.DOTALL):
        raise ValueError("Only SELECT queries are allowed.")
    if ";" in sql.strip().rstrip(";"):
        raise ValueError("Multiple statements are not allowed.")

    # 2) Enforce LIMIT if missing
    if re.search(r"\blimit\b", sql, re.IGNORECASE) is None:
        sql = sql.rstrip().rstrip(";") + f" LIMIT {MAX_ROWS}"

    # 3) Check schemas ONLY for real tables after FROM/JOIN
    #    (aliases like "y.", "zpu." should NOT be treated as schemas)
    tables = re.findall(r"\b(?:from|join)\s+([a-zA-Z0-9_\.]+)", sql, flags=re.IGNORECASE)
    for t in tables:
        # strip any quoting
        t = t.strip().strip('"').strip("`").strip("'")
        if "." in t:
            schema = t.split(".")[0]
            if schema not in ALLOWED_SCHEMAS:
                raise ValueError(f"Query must use schemas in {sorted(ALLOWED_SCHEMAS)}")
        # if no schema prefix, it's fine (we set Database=nyc_taxi in Athena)

    # 4) Optional: block a few risky keywords even inside SELECTs
    banned = re.findall(r"\b(unload|create|drop|delete|update|insert|grant|msck|alter)\b", sql, re.IGNORECASE)
    if banned:
        raise ValueError("Only read-only SELECT queries are allowed.")

    return sql

def run_athena(sql: str):
    q = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_S3},
        WorkGroup=ATHENA_WORKGROUP
    )
    qid = q["QueryExecutionId"]
    # wait loop
    while True:
        resp = athena.get_query_execution(QueryExecutionId=qid)
        state = resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED","FAILED","CANCELLED"):
            break
        time.sleep(0.8)
    if state != "SUCCEEDED":
        reason = resp["QueryExecution"]["Status"].get("StateChangeReason","")
        raise RuntimeError(f"Athena query failed: {state} {reason}")

    # fetch first MAX_ROWS as CSV
    res = athena.get_query_results(QueryExecutionId=qid, MaxResults=1000)
    headers = [col["VarCharValue"] for col in res["ResultSet"]["Rows"][0]["Data"]]
    rows = []
    for r in res["ResultSet"]["Rows"][1:]:
        rows.append([c.get("VarCharValue") for c in r["Data"]])
    # If more pages, paginate a bit (optional)
    next_token = res.get("NextToken")
    while next_token and len(rows) < MAX_ROWS:
        res = athena.get_query_results(QueryExecutionId=qid, NextToken=next_token, MaxResults=1000)
        for r in res["ResultSet"]["Rows"]:
            rows.append([c.get("VarCharValue") for c in r["Data"]])
            if len(rows) >= MAX_ROWS: break
        next_token = res.get("NextToken")
    return headers, rows

def summarize_with_bedrock(question: str, headers, rows) -> str:
    max_preview = min(30, len(rows))
    preview_tsv = "\n".join(
        ["\t".join(headers)]
        + ["\t".join("" if v is None else str(v) for v in r) for r in rows[:max_preview]]
    )

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 150,
        "system": [{"type": "text", "text": "You are a data analyst. Write a crisp, 2–3 sentence insight summary for executives."}],
        "messages": [
            {"role": "user", "content": [{
                "type": "text",
                "text": f"Question: {question}\n\nFirst {max_preview} result rows (TSV):\n{preview_tsv}\n\nWrite a concise insight summary. Avoid speculation."
            }]}
        ],
    }

    out = invoke_bedrock_with_retry(body)
    return "".join(b.get("text", "") for b in out.get("content", []) if b.get("type") == "text").strip()

def lambda_handler(event, context):
    try:
        if event.get("isBase64Encoded"):
            # API Gateway proxy compatibility
            body = json.loads(base64.b64decode(event["body"]))
        else:
            body = event.get("body")
            if isinstance(body, str):
                body = json.loads(body)
            if body is None:
                body = event  # allow direct test with raw event

        question = body.get("question") or body.get("q")
        if not question:
            return {"statusCode": 400, "body": json.dumps({"error":"Missing 'question' field"})}

        sql = call_bedrock_for_sql(question)
        sql = sanitize_sql(sql)

        headers, rows = run_athena(sql)
        narrative = summarize_with_bedrock(question, headers, rows)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "question": question,
                "sql": sql,
                "columns": headers,
                "rows": rows,
                "row_count": len(rows),
                "narrative": narrative
            })
        }
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
