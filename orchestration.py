import os
from datetime import datetime
import pandas as pd
import numpy as np
from pymongo import UpdateOne
import streamlit as st
import saspy
import matplotlib.pyplot as plt

# Your modules
from Data_API.db import db, posts_collection, daily_collection  # :contentReference[oaicite:3]{index=3}
from Data_API.reddit_api import insert_posts                    # :contentReference[oaicite:4]{index=4}
from Agents.post_analysis import insert_analysis                # :contentReference[oaicite:5]{index=5}

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
    docs = list(coll.find({"topic": topic}, {"_id":0, "date":1, "forecast":1, "l95":1, "u95":1}))
    if not docs:
        return pd.DataFrame(columns=["date","forecast","l95","u95"])
    return pd.DataFrame(docs).sort_values("date")

def plot_series(daily_df: pd.DataFrame, f_df: pd.DataFrame):
    fig, ax = plt.subplots()
    if not daily_df.empty:
        ax.plot(pd.to_datetime(daily_df["date"]), daily_df["sentiment_index"], marker="o", label="Daily index")
    if not f_df.empty and "forecast" in f_df:
        x = pd.to_datetime(f_df["date"])
        ax.plot(x, f_df["forecast"], linestyle="--", label="SAS forecast")
        if {"l95","u95"}.issubset(set(f_df.columns)):
            ax.fill_between(x, f_df["l95"], f_df["u95"], alpha=0.2, label="95% band")
    ax.set_ylabel("Sentiment (−1..1)")
    ax.set_xlabel("Date")
    ax.set_title("30-day Sentiment & Forecast")
    ax.grid(True, alpha=0.3)
    ax.legend()
    st.pyplot(fig)

def run_sas_forecast(topic: str):
    sas = saspy.SASsession(cfgname="viya")
    sas.symput("TOPIC", topic)
    sas.symput("HORIZON", "14")
    log = sas.submit("""
        %include '/path/to/sas/fetch_forecast_push.sas';
    """)
    return log['LOG']

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

if st.button("4) Run SAS Forecast"):
    with st.spinner("Running SAS forecast..."):
        log = run_sas_forecast(topic)
    st.text_area("SAS Log", log, height=250)

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

st.markdown("**Step 4 (Forecast):** open `sas/fetch_forecast_push.sas` in Viya Workbench, set `TOPIC`, run it, then refresh this page to see the overlay.")
st.markdown("**Note:** this is a demo app. The analysis quality depends on the Gemini model and prompt, which you can customize in `Agents/post_analysis.py`.")