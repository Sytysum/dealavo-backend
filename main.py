
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import pandas as pd
import xml.etree.ElementTree as ET
import asyncio
import redis.asyncio as redis
import json
import os
from base64 import b64encode
from typing import List
import io

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # lub konkretnie: ["https://dealavo-frontend.onrender.com"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- CONFIG ---
FEED_URL = "https://bizneslink.iterra.pl/api/vobis/prices"
DEALAVO_URL = "https://app.dealavo.com/files/flat_reports/current_dealavo_flat_prices.csv?account_id=3432958&api_key=44RkwUCpjV2xwPyJpeztvHsVP0hIxz9Q"
CACHE_TTL = 900  # 15 minut
REDIS_HOST = "localhost"
REDIS_PORT = 6379
TRACKED_PRODUCTS_KEY = "tracked_products"

FEED_USERNAME = os.getenv("FEED_USERNAME", "vobis")
FEED_PASSWORD = os.getenv("FEED_PASSWORD", "JPkkJ887h64da#dasss@@4f56Asawnchasd6hP")

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# --- MODELE ---
class ProductRequest(BaseModel):
    erp_id: str
    ean: str

class TrackRequest(BaseModel):
    ean: str

# --- HELPERS ---
async def fetch_authenticated_feed_data():
    cache_key = "feed_cache"
    cached = await redis_client.get(cache_key)
    if cached:
        return pd.read_json(cached)

    auth_header = b64encode(f"{FEED_USERNAME}:{FEED_PASSWORD}".encode()).decode()
    headers = {"Authorization": f"Basic {auth_header}"}

    async with httpx.AsyncClient() as client:
        response = await client.get(FEED_URL, headers=headers)
        response.raise_for_status()
        root = ET.fromstring(response.text)

    data = []
    for product in root.findall("product"):
        data.append({
            "ean": product.findtext("ean"),
            "price": float(product.findtext("price", default=0)),
            "stock": int(product.findtext("stock", default=0))
        })

    df = pd.DataFrame(data)
    await redis_client.set(cache_key, df.to_json(), ex=CACHE_TTL)
    return df

async def fetch_dealavo_data(ean: str):
    cache_key = f"dealavo_{ean}"
    cached = await redis_client.get(cache_key)
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
    await redis_client.sadd(TRACKED_PRODUCTS_KEY, req.ean)
    return {"message": f"Produkt {req.ean} dodany do obserwowanych."}

@app.get("/tracked")
async def list_tracked_products():
    eans = await redis_client.smembers(TRACKED_PRODUCTS_KEY)
    result = []
    for ean in eans:
        try:
            data = await get_combined_product_data(ean)
            result.append(data)
        except:
            continue
    return result
