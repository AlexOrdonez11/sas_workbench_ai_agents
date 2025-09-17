import os
from datetime import datetime
import pandas as pd
import numpy as np
from pymongo import UpdateOne
import streamlit as st
import saspy
from pathlib import Path
import matplotlib.pyplot as plt

# Your modules
from Data_API.db import db, posts_collection, daily_collection  
from Data_API.reddit_api import insert_posts                   
from Agents.post_analysis import insert_analysis               

st.set_page_config(page_title="Reddit 30-Day Topic Insight", layout="wide")

# ---------- Helpers ----------
def aggregate_daily_metrics(topic: str):
    """
    Reads analyzed posts for the topic from 'Posts' and upserts 1 doc/day into 'Daily_Metrics'.
    Assumes 'analysis' subdocument exists on each post (from insert_analysis()).
    """
    cur = posts_collection.find(
        {"topic": topic, "analysis": {"$exists": True}},
        {"date": 1, "analysis": 1}
    )
    rows = list(cur)
    if not rows:
        return 0

    df = pd.DataFrame([{
        "date": r.get("date"),
        "sentiment_score": r["analysis"].get("sentiment_score", 0.0),
        "sentiment_label": r["analysis"].get("sentiment_label", "neutral"),
        "stance": r["analysis"].get("stance", "unclear"),
        "toxicity_flag": bool(r["analysis"].get("toxicity_flag", False)),
    } for r in rows if r.get("date")])

    grp = df.groupby("date")
    daily_docs = []
    for date, g in grp:
        n = len(g)
        if n == 0:
            continue
        sentiment_index = float(g["sentiment_score"].mean())
        def frac(col, val): 
            return float((g[col] == val).mean()) if n else 0.0
        label_dist = {
            "positive": frac("sentiment_label", "positive"),
            "neutral":  frac("sentiment_label", "neutral"),
            "negative": frac("sentiment_label", "negative"),
        }
        stance_dist = {
            "supportive": frac("stance", "supportive"),
            "mixed":      frac("stance", "mixed"),
            "skeptical":  frac("stance", "skeptical"),
            "unclear":    frac("stance", "unclear"),
        }
        tox_rate = float(g["toxicity_flag"].mean())
        daily_docs.append({
            "topic": topic,
            "date": date,
            "sentiment_index": sentiment_index,
            "label_dist": label_dist,
            "stance_dist": stance_dist,
            "toxicity_rate": tox_rate,
            "updated_at": datetime.utcnow()
        })

    # Upsert to Daily_Metrics
    ops = [UpdateOne({"topic": topic, "date": d["date"]}, {"$set": d}, upsert=True) for d in daily_docs]
    if ops:
        daily_collection.bulk_write(ops, ordered=False)
    return len(daily_docs)

def load_daily(topic: str) -> pd.DataFrame:
    docs = list(daily_collection.find({"topic": topic}, {"_id":0, "date":1, "sentiment_index":1}))
    if not docs:
        return pd.DataFrame(columns=["date","sentiment_index"])
    df = pd.DataFrame(docs).sort_values("date")
    return df

def load_forecast(topic: str) -> pd.DataFrame:
    coll = db["forecasts"]
    docs = list(coll.find({"topic": topic}, {"_id":0, "date":1, "forecast":1, "error":1}))
    if not docs:
        return pd.DataFrame(columns=["date","forecast","error"])
    return pd.DataFrame(docs).sort_values("date")

def plot_series(daily_df: pd.DataFrame, f_df: pd.DataFrame):
    fig, ax = plt.subplots()
    if not daily_df.empty:
        ax.plot(pd.to_datetime(daily_df["date"]), daily_df["sentiment_index"], marker="o", label="Daily index")
    if not f_df.empty and "forecast" in f_df:
        x = pd.to_datetime(f_df["date"])
        ax.plot(x, f_df["forecast"], linestyle="--", label="SAS forecast")
    ax.set_ylabel("Sentiment (−1..1)")
    ax.set_xlabel("Date")
    ax.set_title("30-day Sentiment & Forecast")
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    ax.legend()
    st.pyplot(fig)

def run_sas_forecast_separated(topic: str, sasfile_path: Path, horizon: int = 5) -> pd.DataFrame:
    """
    Orchestrates SAS forecasting using a separate .sas file (no PROC PYTHON inside SAS).
    Returns the forecast DataFrame with columns [date, forecast, l95, u95].
    """
    # --- 1) Read daily metrics from Mongo ---
    rows = list(db["Daily_Metrics"].find(
        {"topic": topic}, {"_id": 0, "date": 1, "sentiment_index": 1}))
    if not rows:
        raise RuntimeError(f"No Daily_Metrics for topic '{topic}'. Run aggregation first.")
    df = pd.DataFrame(rows).sort_values("date")

    # Convert to SAS date
    df["date_sas"] = pd.to_datetime(df["date"])
    df["date_sas"] = (df["date_sas"] - pd.Timestamp("1960-01-01")) // pd.Timedelta(days=1)
    df = df[["date", "date_sas", "sentiment_index"]]

    # --- 2) Open SAS session and upload WORK.DAILY ---
    sas = saspy.SASsession()
    sas.df2sd(df, table="DAILY", libref="WORK")


    # --- 3) Run the pure-SAS forecast file ---
    sas.symput("HORIZON", str(horizon))
    sas.symput("SASFILE", str(sasfile_path))
    log = sas.submit(r'''
        %put NOTE: Including &SASFILE.;
        %include "&SASFILE.";
    ''')["LOG"]


    print(log)

    # --- 4) Pull results back to Python ---
    fcast = sas.sd2df(table="to_write",libref="work")
    if fcast.empty:
        raise RuntimeError("Forecast returned no rows (check SAS log and input data).")

    # --- 5) Write back to Mongo (forecasts collection) ---
    ops = []

    df["date_n"] = pd.to_datetime(df["date"])

    last_date = df["date_n"].max()

    future_dates = np.append(df["date"].values,[(last_date + pd.Timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, horizon+1)])

    for i, r in fcast.iterrows():
        ops.append({
            "topic": topic,
            "date": future_dates[i],
            "forecast":r["PREDICT"],
            "error":r["ERROR"]
        })
    if ops:
        print (ops)
        result= db["forecasts"].insert_many(ops)
        print ("inserted count: ", len(result.inserted_ids))

    return fcast

# ---------- UI ----------
st.title("🔎 Reddit Topic Insight (30-day on-demand)")

topic = st.text_input("Topic", value="iPhone 17")
col1, col2, col3 = st.columns([1,1,2])

with col1:
    if st.button("1) Collect posts (top-5/day, last month)"):
        try:
            insert_posts(topic) 
            st.success("Posts loaded.")
        except Exception as e:
            st.error(f"Error loading posts: {e}")

with col2:
    if st.button("2) Analyze posts (Gemini)"):
        try:
            out = insert_analysis(topic)
            st.success("Analysis complete.")
        except Exception as e:
            st.error(f"Error analyzing: {e}")

st.markdown("---")

if st.button("3) Aggregate daily metrics"):
    try:
        n = aggregate_daily_metrics(topic)
        st.success(f"Aggregated {n} day(s).")
    except Exception as e:
        st.error(f"Aggregation failed: {e}")

daily_df = load_daily(topic)
f_df = load_forecast(topic)

if daily_df.empty:
    st.info("No daily metrics yet. Run steps 1–3.")
else:
    plot_series(daily_df, f_df)

    # day drill-down
    day = st.selectbox("Inspect day", daily_df["date"].tolist())
    posts = list(posts_collection.find({"topic": topic, "date": day}, {"_id":0, "subreddit":1, "score":1, "num_comments":1, "title":1, "url":1, "analysis":1}).sort("score", -1))
    if posts:
        df = pd.DataFrame([{
            "Subreddit": p.get("subreddit"),
            "Score": p.get("score"),
            "Comments": p.get("num_comments"),
            "Title": p.get("title"),
            "URL": p.get("url"),
            "Sentiment": (p.get("analysis") or {}).get("sentiment_score"),
            "Label": (p.get("analysis") or {}).get("sentiment_label"),
            "Stance": (p.get("analysis") or {}).get("stance"),
            "Themes": ", ".join((p.get("analysis") or {}).get("key_themes", [])),
            "Toxic?": (p.get("analysis") or {}).get("toxicity_flag"),
        } for p in posts])
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.write("No posts for that day yet.")

if st.button("4) Run SAS Forecast (separated .sas)"):
    try:
        SASFILE = Path(__file__).resolve().parent / "sas_tools" / "forecast.sas"
        fc = run_sas_forecast_separated(topic, SASFILE, horizon=5)
        st.success(f"Forecast rows: {len(fc)} (written to 'forecasts').")
    except Exception as e:
        st.error(f"SAS forecast failed: {e}")

st.markdown("**Note:** this is a demo app. The analysis quality depends on the Gemini model and prompt, which you can customize in `Agents/post_analysis.py`.")