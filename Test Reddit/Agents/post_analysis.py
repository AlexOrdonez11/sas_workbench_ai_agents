import google.generativeai as genai
import os, re
import json
from datetime import datetime
from pydantic import BaseModel, Field
from Data_API.db import posts_collection as posts_coll, daily_collection as daily_coll

# declaring gemini variables
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
GEM_MODEL = genai.GenerativeModel(model_name="gemini-2.5-flash")


class Analysis(BaseModel):
    sentiment_score: float = Field(..., ge=-1, le=1)
    sentiment_label: str   # positive|neutral|negative
    stance: str            # supportive|skeptical|mixed|unclear
    key_themes: list[str] = []
    toxicity_flag: bool = False

SYSTEM = """
You are a JSON-only generator. Analyze the given text and return **valid UTF-8 JSON** only.
Required schema:
{
  "sentiment_score": number between -1 and 1,
  "sentiment_label": "positive" | "neutral" | "negative",
  "stance": "supportive" | "skeptical" | "mixed" | "unclear",
  "key_themes": [array of short English phrases only, no quotes or extra tokens],
  "toxicity_flag": boolean
}
- Do not add any other keys or text.
- Do not include trailing commas or language other than English inside key_themes.
- If unsure about a theme, omit it rather than writing free text.
Return nothing but the JSON object.
"""

# function in progress to parallelize the reviews analysis
def make_batch_payload(items):
    # items: list[{"id": "...", "title": "...", "selftext": "..."}]
    lines = [SYSTEM]
    for it in items:
        text = (it.get("title","") + "\n" + it.get("selftext","")).strip()
        text = text[:MAX_TEXT]
        # delimit each item clearly so the model can separate them
        lines.append(json.dumps({"id": it["id"], "text": text}, ensure_ascii=False))
    return "\n".join(lines)

def clean_and_parse_json(raw_str: str):
    """
    Cleans code fences (```json ... ```) from a string and parses it as JSON.
    """
    if raw_str is None:
        raise ValueError("No input string")
    s = raw_str.strip()
    s = re.sub(r"^```(?:json)?\s*|\s*```$", "", s)           # strip fences
    m = re.search(r"\{.*\}\s*$", s, flags=re.S)               # grab last JSON object
    if m: s = m.group(0)
    try:
        return json.loads(s)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format: {e}")

def analyze_text(text: str) -> dict:
    # Call Gemini model and handle response
    if not text.strip():
        return {
            "sentiment_score": 0.0,
            "sentiment_label": "neutral",
            "stance": "unclear",
            "key_themes": [],
            "toxicity_flag": False
        }
    resp = GEM_MODEL.generate_content(text)
    print (resp)
    try:
        return clean_and_parse_json(resp.text)
    except Exception as e:
        print("Error parsing response")
        print(resp.text)
        print(e)
        raise ValueError(f"Invalid JSON format: {e}")
        # return {
        #     "sentiment_score": 0.0,
        #     "sentiment_label": "neutral",
        #     "stance": "unclear",
        #     "key_themes": [],
        #     "toxicity_flag": False
        # }

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