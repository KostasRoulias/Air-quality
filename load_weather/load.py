import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text

# 1) Read processed data (container path)
df_final = pd.read_csv("data/processed/weather_air_transformed.csv")

df_final = df_final.rename(columns={
    "lat": "latitude",
    "lon": "longitude",
    "weather_desc": "weather_description",
})

df_final["time"] = pd.to_datetime(df_final["time"], errors="coerce")
df_final = df_final.drop_duplicates(subset=["city", "time"])

print("Rows to load:", len(df_final))

# 2) Save CSV inside container
os.makedirs("/app/csv", exist_ok=True)
df_final.to_csv("/app/csv/weather_data.csv", index=False)

# 3) DB connection
engine = create_engine("postgresql+psycopg2://postgres:docker@postgres-db:5432/postgres")

create_sql = """
CREATE TABLE IF NOT EXISTS weather_data (
    id BIGSERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    latitude  DECIMAL(9,6),
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
    o3  DECIMAL(8,2)
);
"""

with engine.begin() as conn:
    conn.execute(text(create_sql))
    conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_weather_city_time
        ON weather_data (city, time);
    """))

    # staging + insert only new
    conn.execute(text("DROP TABLE IF EXISTS weather_data_stg;"))
    df_final.to_sql("weather_data_stg", con=conn, if_exists="replace", index=False, method="multi")

    conn.execute(text("""
        INSERT INTO weather_data (
            city, latitude, longitude, time, temp_c, humidity_pct, wind_speed_ms,
            weather_code, weather_description, aqi_eu, pm25, no2, o3
        )
        SELECT
            city, latitude, longitude, time, temp_c, humidity_pct, wind_speed_ms,
            weather_code, weather_description, aqi_eu, pm25, no2, o3
        FROM weather_data_stg
        ON CONFLICT (city, time) DO NOTHING;
    """))

with engine.connect() as conn:
    out = pd.read_sql_query("SELECT * FROM weather_data ORDER BY time DESC, city LIMIT 10;", conn)

print(out)
