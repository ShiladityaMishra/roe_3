from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import json
import re

# Load your data from data.json into a DataFrame
with open("data.json") as f:
    raw_data = json.load(f)

df = pd.DataFrame(raw_data)

# Ensure correct data types and lowercase column names
df.columns = [col.lower() for col in df.columns]
df['date'] = pd.to_datetime(df['date'])
df['rep'] = df['rep'].astype(str)
df['city'] = df['city'].astype(str)
df['product'] = df['product'].astype(str)
df['region'] = df['region'].astype(str)
df['sales'] = pd.to_numeric(df['sales'], errors='coerce')

app = FastAPI()

# Enable CORS for all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/query")
async def query(q: str, request: Request):
    answer = answer_query(q)
    headers = {"X-Email": "21f3000037@ds.study.iitm.ac.in"}
    return JSONResponse(content={"answer": answer}, headers=headers)

def answer_query(q: str):
    q = q.lower().strip()

    try:
        if "total sales of" in q:
            m = re.search(r"total sales of (.+?) in (.+)", q)
            product, city = m.group(1).strip().lower(), m.group(2).strip().lower()
            result = df[(df['product'].str.lower() == product) & (df['city'].str.lower() == city)]
            return round(result['sales'].sum(), 2)

        elif "how many sales reps are there in" in q:
            m = re.search(r"sales reps are there in (.+)", q)
            region = m.group(1).strip().lower()
            result = df[df['region'].str.lower() == region]
            return result['rep'].nunique()

        elif "average sales for" in q:
            m = re.search(r"average sales for (.+?) in (.+)", q)
            product, region = m.group(1).strip().lower(), m.group(2).strip().lower()
            result = df[(df['product'].str.lower() == product) & (df['region'].str.lower() == region)]
            return round(result['sales'].mean(), 2)

        elif "on what date did" in q and "make the highest sale in" in q:
            m = re.search(r"on what date did (.+?) make the highest sale in (.+)", q)
            rep, city = m.group(1).strip().lower(), m.group(2).strip().lower()
            result = df[(df['rep'].str.lower() == rep) & (df['city'].str.lower() == city)]
            if result.empty:
                return "No sales found"
            max_row = result.loc[result['sales'].idxmax()]
            return str(max_row['date'].date())

        else:
            return "Unsupported query format."

    except Exception as e:
        return f"Error processing query: {str(e)}"
