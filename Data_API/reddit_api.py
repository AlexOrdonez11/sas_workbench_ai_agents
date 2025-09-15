import praw
import os
import pandas as pd
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

def fetch_pool(topic: str) -> pd.DataFrame:
    rows = []
    for s in reddit.subreddit("all").search(
        query=topic, sort="top", time_filter=TIME_FILTER, syntax="lucene", limit=MAX_PULL
    ):
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
    posts_coll.bulk_write(ops, ordered=False)

print(f"Days: {top5['date'].nunique()}  Docs upserted: {len(ops)}")