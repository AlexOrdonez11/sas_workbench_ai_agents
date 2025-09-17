import praw
import os, re
import pandas as pd
from datetime import datetime, timezone
from pymongo import UpdateOne
from Data_API.db import posts_collection
# Create a Reddit instance
reddit = praw.Reddit(
    client_id=os.getenv("reddit_id"),
    client_secret=os.getenv("reddit_secret"),
    user_agent="Agentic AI test by Alex",
    username=os.getenv("reddit_user"),
    password=os.getenv("reddit_pass")
)

# Test the connection: print your own username
print(reddit.user.me())

def lucene_query_for_topic(topic: str) -> str:
    """
    Build a strict Lucene query targeting both title and selftext.
    Handles phrases and a compact variant (no spaces/hyphens).
    """
    t = topic.strip()
    esc = t.replace('"', r'\"')                 # escape quotes
    compact = re.sub(r'[\s\-]+', '', esc)       # e.g., "iPhone 17" -> "iPhone17"
    terms = [f'title:"{esc}"', f'selftext:"{esc}"', f'"{esc}"']
    if compact.lower() != esc.lower():
        terms.append(f'"{compact}"')
    return "(" + " OR ".join(terms) + ")"

def topic_regex(topic: str) -> re.Pattern:
    """
    Build a flexible regex that matches the topic across spaces/hyphens.
    e.g., "iPhone 17 Pro" matches "iphone-17  pro".
    """
    tokens = re.findall(r'\w+', topic, flags=re.UNICODE)
    if not tokens:
        tokens = [topic]
    pattern = r"\b" + r"[\s\-]*".join(map(re.escape, tokens)) + r"\b"
    return re.compile(pattern, re.IGNORECASE)

def fetch_pool(topic: str) -> pd.DataFrame:
    QUERY = lucene_query_for_topic(topic)       # topic is the free text input from the user
    TITLE_BODY_MATCH = topic_regex(topic)
    rows = []
    for s in reddit.subreddit("all").search(
        query=QUERY, sort="top", time_filter="month", syntax="lucene", limit=None
    ):
        title = s.title or ""
        selftext = s.selftext or ""
        if not (TITLE_BODY_MATCH.search(title) or TITLE_BODY_MATCH.search(selftext)):
            continue
        rows.append({
            "submission_id": f"t3_{s.id}",
            "topic": topic,
            "title": s.title or "",
            "selftext": s.selftext or "",
            "url": f"https://www.reddit.com{s.permalink}",
            "subreddit": str(s.subreddit),
            "score": int(s.score or 0),
            "num_comments": int(s.num_comments or 0),
            "created_utc": float(s.created_utc or 0),
            "date": datetime.fromtimestamp(s.created_utc, tz=timezone.utc).date().isoformat(),
            "over_18": bool(getattr(s, "over_18", False)),
            "fetched_at": datetime.utcnow()
        })
    return pd.DataFrame(rows)

def insert_posts(TOPIC:str):
    
    pool = fetch_pool(TOPIC)
    pool = pool[pool["over_18"] == False].copy()

    # top-5 per day by score
    top5 = (pool.sort_values(["date","score"], ascending=[True, False])
                .groupby("date").head(5).reset_index(drop=True))

    # Upsert posts (idempotent by submission_id)
    ops = []
    for doc in top5.to_dict("records"):
        sid = doc["submission_id"]
        ops.append(UpdateOne(
            {"submission_id": sid},
            {"$set": {**doc}},
            upsert=True
        ))
    if ops:
        posts_collection.bulk_write(ops, ordered=False)

    print(f"Days: {top5['date'].nunique()}  Docs upserted: {len(ops)}")