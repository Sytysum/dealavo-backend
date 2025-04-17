
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
CACHE_TTL = 900
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
TRACKED_PRODUCTS_KEY = "tracked_products"

FEED_USERNAME = os.getenv("FEED_USERNAME", "vobis")
FEED_PASSWORD = os.getenv("FEED_PASSWORD", "JPkkJ887h64da#dasss@@4f56Asawnchasd6hP")

redis_client = redis.from_url(REDIS_URL, decode_responses=True)

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
        response = await client.get(FEED_URL, headers=headers, timeout=30.0)
        response.raise_for_status()
        root = ET.fromstring(response.text)

    data = []
    for product in root.findall("Produkt"):
        data.append({
            "ean": product.findtext("KOD_KRESKOWY"),
            "price": float(product.findtext("CENA_WWW_BRUTTO", default="0")),
            "stock": int(product.findtext("dostepnosc", default="0"))
        })

    df = pd.DataFrame(data)
    await redis_client.set(cache_key, df.to_json(), ex=CACHE_TTL)
    return df

async def get_combined_product_data(ean: str, erp_id: str = ""):
    feed_df = await fetch_authenticated_feed_data()
    feed_row = feed_df[feed_df["ean"] == ean]

    if feed_row.empty:
        raise HTTPException(status_code=404, detail="Produkt nie znaleziony w feedzie")

    feed_data = feed_row.iloc[0].to_dict()

    return {
        "erp_id": erp_id,
        "ean": ean,
        "your_feed": feed_data,
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
