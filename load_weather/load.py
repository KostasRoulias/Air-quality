import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os

# =====================
# CONFIG
# =====================
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
AIR_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

CITIES = [
    ("Lisbon", 38.7167, -9.1333),
    ("Thessaloniki", 40.6436, 22.9309),
    ("Brussels", 50.8505, 4.3488),
    ("Barcelona", 41.3888, 2.1590),
    ("Berlin", 52.5244, 13.4105),
]

DB_URL = "postgresql+psycopg2://postgres:docker@postgres-db:5432/postgres"
CSV_OUT = "/app/csv/weather_data.csv"

WEATHER_MAP = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Rime fog",
    51: "Light drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    80: "Rain showers",
    95: "Thunderstorm",
}

# =====================
# EXTRACT
# =====================
rows = []

for city, lat, lon in CITIES:
    w = requests.get(
        WEATHER_URL,
        params={
            "latitude": lat,
            "longitude": lon,
            "current": "temperature_2m,relative_humidity_2m,weather_code,wind_speed_10m",
            "timezone": "Europe/Athens",
        },
        timeout=30,
    ).json()["current"]

    a = requests.get(
        AIR_URL,
        params={
            "latitude": lat,
            "longitude": lon,
            "current": "european_aqi,pm2_5,nitrogen_dioxide,ozone",
            "timezone": "Europe/Athens",
        },
        timeout=30,
    ).json()["current"]

    rows.append({
        "city": city,
        "latitude": lat,
        "longitude": lon,
        "time": w.get("time"),
        "temp_c": w.get("temperature_2m"),
        "humidity_pct": w.get("relative_humidity_2m"),
        "wind_speed_ms": w.get("wind_speed_10m"),
        "weather_code": w.get("weather_code"),
        "aqi_eu": a.get("european_aqi"),
        "pm25": a.get("pm2_5"),
        "no2": a.get("nitrogen_dioxide"),
        "o3": a.get("ozone"),
    })

df = pd.DataFrame(rows)

# =====================
# TRANSFORM
# =====================
df["time"] = pd.to_datetime(df["time"], errors="coerce")
df["weather_description"] = df["weather_code"].map(WEATHER_MAP).fillna("Unknown")

df = df.drop_duplicates(subset=["city", "time"])
df = df.round(2)

# save CSV 
os.makedirs("/app/csv", exist_ok=True)
df.to_csv(CSV_OUT, index=False)

print(f"[ETL] Rows extracted & transformed: {len(df)}")

# =====================
# LOAD
# =====================
engine = create_engine(DB_URL)

create_sql = """
CREATE TABLE IF NOT EXISTS weather_data (
    id BIGSERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    time TIMESTAMP,
    temp_c DECIMAL(5,2),
    humidity_pct INT,
    wind_speed_ms DECIMAL(6,2),
    weather_code INT,
    weather_description TEXT,
    aqi_eu INT,
    pm25 DECIMAL(6,2),
    no2 DECIMAL(8,2),
    o3 DECIMAL(8,2)
);
"""

with engine.begin() as conn:
    conn.execute(text(create_sql))
    conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_weather_city_time
        ON weather_data (city, time);
    """))

    # staging
    conn.execute(text("DROP TABLE IF EXISTS weather_data_stg;"))
    df.to_sql("weather_data_stg", conn, if_exists="replace", index=False, method="multi")

    # upsert logic
    conn.execute(text("""
        INSERT INTO weather_data (
            city, latitude, longitude, time,
            temp_c, humidity_pct, wind_speed_ms,
            weather_code, weather_description,
            aqi_eu, pm25, no2, o3
        )
        SELECT
            city, latitude, longitude, time,
            temp_c, humidity_pct, wind_speed_ms,
            weather_code, weather_description,
            aqi_eu, pm25, no2, o3
        FROM weather_data_stg
        ON CONFLICT (city, time) DO NOTHING;
    """))

print("[ETL] Load completed successfully")

# verification
with engine.connect() as conn:
    result = pd.read_sql("SELECT COUNT(*) FROM weather_data;", conn)

print(result)
