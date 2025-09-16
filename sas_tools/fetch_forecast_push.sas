/* ================== CONFIG ================== */
%let TOPIC = iPhone 17;           /* <-- change per run */
%let HORIZON = 14;                /* forecast days ahead */

/* ENV from Workbench -> use PROC PYTHON to read Mongo & later write back */
proc python;
submit;
import os, pandas as pd
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "Reddit_Agent")
TOPIC     = SAS.symget('TOPIC')

cli = MongoClient(MONGO_URI)
db  = cli[MONGO_DB]

# Pull daily_metrics for this TOPIC (last 30 days already computed by your pipeline)
docs = list(db["Daily_Metrics"].find({"topic": TOPIC}, {"_id":0, "date":1, "sentiment_index":1}))
df = pd.DataFrame(docs).sort_values("date")
if df.empty:
    raise SystemExit("No daily_metrics for this topic. Run Streamlit pipeline first.")

# Prepare date_sas (days since 1960-01-01)
df["date_sas"] = pd.to_datetime(df["date"])
df["date_sas"] = (df["date_sas"] - pd.Timestamp("1960-01-01")) // pd.Timedelta(days=1)

# Hand off to SAS as WORK.DAILY
SAS.df2sd(df[["date","date_sas","sentiment_index"]], "DAILY", libref="WORK")
endsubmit;
run;

/* -------- Forecast with PROC ESM (damped trend) -------- */
proc sort data=work.daily; by date_sas; run;

proc esm data=work.daily out=work.sent_forecast lead=&HORIZON. back=0 print=none;
  id date_sas interval=day;
  forecast sentiment_index / model=damptrend transform=None;
run;

/* Keep only future rows that have forecast and prepare ISO date */
data work.to_write;
  set work.sent_forecast;
  if missing(forecast)=0; /* future points */
  length date $10;
  date = put(date_sas, yymmdd10.);
  keep date forecast l95 u95;
run;

/* -------- Push forecast rows back to Mongo (collection: forecasts) -------- */
proc python;
submit;
import os, math, pandas as pd
from pymongo import MongoClient, UpdateOne

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "Reddit_Agent")
TOPIC     = SAS.symget('TOPIC')

# Pull the WORK.TO_WRITE table from SAS
to_write = SAS.sd2df("WORK.TO_WRITE")

cli = MongoClient(MONGO_URI)
db  = cli[MONGO_DB]
coll = db["forecasts"]

ops = []
for _, r in to_write.iterrows():
    ops.append(UpdateOne(
        {"topic": TOPIC, "date": r["date"]},
        {"$set": {
            "topic": TOPIC,
            "date": r["date"],
            "forecast": float(r["forecast"]),
            "l95": float(r["l95"]) if not pd.isna(r["l95"]) else None,
            "u95": float(r["u95"]) if not pd.isna(r["u95"]) else None
        }},
        upsert=True
    ))
if ops:
    coll.bulk_write(ops, ordered=False)
print(f"Upserted {len(ops)} forecast rows for topic '{TOPIC}'.")
endsubmit;
run;

/* (Optional) quick print */
proc print data=work.to_write(obs=20); run;