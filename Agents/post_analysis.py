import google.generativeai as genai
import os
import json
from datetime import datetime
from pydantic import BaseModel, Field
from db import posts_collection as posts_coll, daily_collection as daily_coll

genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
GEM_MODEL = genai.GenerativeModel(model_name="gemini-2.5-flash")


class Analysis(BaseModel):
    sentiment_score: float = Field(..., ge=-1, le=1)
    sentiment_label: str   # positive|neutral|negative
    stance: str            # supportive|skeptical|mixed|unclear
    key_themes: list[str] = []
    toxicity_flag: bool = False

SYSTEM = """You analyze social posts about a topic.
Return strict JSON with:
- sentiment_score (-1..1), sentiment_label (positive|neutral|negative)
- stance (supportive|skeptical|mixed|unclear)
- key_themes (array of short phrases)
- toxicity_flag (true/false)
JSON only.
"""

def analyze_text(text: str) -> dict:
    resp = GEM_MODEL.generate_content(SYSTEM + "\n---\n" + text[:8000])
    try:
        return json.loads(resp.text)
    except Exception:
        return {
            "sentiment_score": 0.0,
            "sentiment_label": "neutral",
            "stance": "unclear",
            "key_themes": [],
            "toxicity_flag": False
        }

def insert_analysis(TOPIC: str) -> dict:
    # Fetch posts for the topic
    docs = list(posts_coll.find({"topic": TOPIC}, {"submission_id":1,"title":1,"selftext":1,"date":1,"topic":1}))
    if not docs:
        return {"error": "No posts found for the topic."} 
    print(f"Posts to analyze: {len(docs)}")
    # Analyze each post and upsert results
    for d in docs:
        text = (d.get("title","") + "\n" + d.get("selftext","")).strip()
        res = analyze_text(text)
        posts_coll.update_one({
            "submission_id": d["submission_id"]
        }, {
            "$set": {
                "analysis": res,
                "analyzed_at": datetime.utcnow()
            }
        })

    print(f"Analyses upserted: {len(docs)}")