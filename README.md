# SAS Workbench AI Agents

This project demonstrates an end-to-end AI workflow that pulls Reddit data,
analyzes it with LLMs (Google Gemini), and performs time-series forecasting 
with SAS Viya Workbench.  
The whole flow can be launched from a single Streamlit interface.

**This is the first version of the workflow I will try to implement as much as I can (Vector search, parallelization, LangChain/LangGraph, and any other interesting implementation)**

---

## Project structure

sas_workbench_ai_agents/
- Agents/
  - post_analysis.py # Gemini-based sentiment & topic analysis

- Data_API/
  - db.py # MongoDB connection helpers
  - reddit_api.py # Reddit API fetch of top posts/comments
    
- sas_tools/
  - forecast.sas # Pure SAS file: ESM forecast & confidence bands

- orchestration.py # Streamlit app: user input → Reddit fetch → analysis → SAS forecast

- README.md # Project documentation

---

## High-level workflow

1. **User input**  
   - Enter a free-text topic in the Streamlit app.

2. **Data ingestion**  
   - `reddit_api.py` queries Reddit for the last 30 days top posts and comments.
   - `db.py` stores raw and cleaned data in MongoDB.
   - Planning to include RAG vector search.

3. **AI analysis**  
   - `post_analysis.py` batches posts and runs Gemini (gemini-2.5-flash) for  
     sentiment, stance, key themes, toxicity, etc. I will add more agents later
   - Results are written back to MongoDB.

4. **Forecasting with SAS**  
   - Daily sentiment index is uploaded to SAS Viya via `saspy`.
   - `%include sas_tools/forecast.sas` runs `PROC ESM` (damped trend) to forecast
     the next 5 days.
   - Forecasts are pulled back into Python and stored in MongoDB.

5. **Visualization**  
   - Streamlit displays:
     - Daily sentiment time series
     - SAS forecast and confidence intervals
     - Key topic trends and insights
     
PD simple streamlit, will make modifications.

---
