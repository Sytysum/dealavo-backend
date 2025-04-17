
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import pandas as pd
import xml.etree.ElementTree as ET
import asyncio
import redis.asyncio as redis
import json
import os
from typing import List
import io
from fastapi.middleware.cors import CORSMiddleware

print("🚀 Starting Dealavo FastAPI backend...")

app = FastAPI()

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIG ---
FEED_URL = "https://app.dealavo.com/files/uploaded_files/feed_20250417T100321_142.xml?account_id=3432958&api_key=44RkwUCpjV2xwPyJpeztvHsVP0hIxz9Q"
DEALAVO_URL = "https://app.dealavo.com/files/flat_reports/current_dealavo_flat_prices.csv?account_id=3432958&api_key=44RkwUCpjV2xwPyJpeztvHsVP0hIxz9Q"
CACHE_TTL = 900
REDIS_URL = os.getenv("REDIS_URL", "redis://red-d00e3ik9c44c73fj6fig:6379")
TRACKED_PRODUCTS_KEY = "tracked_products"

try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    print("✅ Redis client initialized.")
except Exception as e:
    print("❌ Redis init failed:", e)
    redis_client = None

# --- MODELE ---
class ProductRequest(BaseModel):
    erp_id: str
    ean: str

class TrackRequest(BaseModel):
    ean: str

# --- HELPERS ---
async def fetch_authenticated_feed_data():
    cache_key = "feed_cache"
    cached = await redis_client.get(cache_key) if redis_client else None
    if cached:
        return pd.read_json(cached)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(FEED_URL)
            response.raise_for_status()
    except httpx.RequestError as e:
        print("❌ Błąd podczas pobierania feeda:", e)
        raise HTTPException(status_code=504, detail="Błąd podczas pobierania danych z feeda")

    try:
        root = ET.fromstring(response.text)
    except Exception as e:
        print("❌ Nieprawidłowy XML w feedzie:", e)
        raise HTTPException(status_code=500, detail="Nieprawidłowy XML w feedzie")

    data = []
    for product in root.findall("product"):
        data.append({
            "ean": product.findtext("ean"),
            "price": float(product.findtext("price", default=0)),
            "stock": int(product.findtext("stock", default=0))
        })

    df = pd.DataFrame(data)
    if redis_client:
        await redis_client.set(cache_key, df.to_json(), ex=CACHE_TTL)
    return df

async def fetch_dealavo_data(ean: str):
    cache_key = f"dealavo_{ean}"
    cached = await redis_client.get(cache_key) if redis_client else None
    if cached:
        return json.loads(cached)

    async with httpx.AsyncClient() as client:
        resp = await client.get(DEALAVO_URL)
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail="Dealavo API error")

        df = pd.read_csv(io.StringIO(resp.text))
        df["ean"] = df["ean"].astype(str)
        filtered = df[df["ean"] == ean]

        if not filtered.empty:
            row = filtered.iloc[0]
            result = {
                "lowest_price": row.get("min_price"),
                "seller": row.get("min_price_shop")
            }
            if redis_client:
                await redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL)
            return result

    raise HTTPException(status_code=404, detail="Produkt nie znaleziony w Dealavo")

async def get_combined_product_data(ean: str, erp_id: str = ""):
    feed_df = await fetch_authenticated_feed_data()
    feed_row = feed_df[feed_df["ean"] == ean]

    if feed_row.empty:
        raise HTTPException(status_code=404, detail="Produkt nie znaleziony w feedzie")

    dealavo_data = await fetch_dealavo_data(ean)
    feed_data = feed_row.iloc[0].to_dict()

    price_diff = None
    if dealavo_data["lowest_price"]:
        price_diff = round(
            ((feed_data["price"] - dealavo_data["lowest_price"]) / dealavo_data["lowest_price"]) * 100, 2
        )

    return {
        "erp_id": erp_id,
        "ean": ean,
        "dealavo": dealavo_data,
        "your_feed": feed_data,
        "price_difference_percent": price_diff
    }

# --- ENDPOINTY ---
@app.post("/product")
async def get_product_info(request: ProductRequest):
    return await get_combined_product_data(request.ean, request.erp_id)

@app.post("/track")
async def track_product(req: TrackRequest):
    if redis_client:
        await redis_client.sadd(TRACKED_PRODUCTS_KEY, req.ean)
    return {"message": f"Produkt {req.ean} dodany do obserwowanych."}

@app.get("/tracked")
async def list_tracked_products():
    eans = await redis_client.smembers(TRACKED_PRODUCTS_KEY) if redis_client else []
    result = []
    for ean in eans:
        try:
            data = await get_combined_product_data(ean)
            result.append(data)
        except:
            continue
    return result
