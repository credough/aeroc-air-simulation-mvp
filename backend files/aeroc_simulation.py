from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import pandas as pd
from datetime import datetime
import boto3

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5500"],  # Live Server default port
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class SimulationInput(BaseModel):
    duration_minutes: int
    selected_country: str
    selected_city: str
    selected_activity: str

@app.post("/simulate")
def simulate(input: SimulationInput):
    user_result, _ = run_simulation(
        duration_minutes=input.duration_minutes,
        selected_country=input.selected_country,
        selected_city=input.selected_city,
        selected_activity=input.selected_activity
    )
    return user_result

from fastapi.responses import JSONResponse
import os

@app.get("/choropleth-data")
def get_choropleth_data():
    try:
        if not os.path.exists("province_choropleth_data.csv"):
            return JSONResponse(content={"error": "Choropleth data not found."}, status_code=404)
        
        df = pd.read_csv("province_choropleth_data.csv")
        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/bar-chart-data")
def get_bar_chart_data():
    try:
        if not os.path.exists("all_simulated_data.csv"):
            return JSONResponse(content={"error": "Bar chart data not found."}, status_code=404)
        
        df = pd.read_csv("all_simulated_data.csv")
        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


def upload_to_s3(file_name, bucket_name, object_name=None):
    if object_name is None:
        object_name = file_name

    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket_name, object_name)
        print(f"Uploaded {file_name} to S3 bucket '{bucket_name}' as '{object_name}'")
    except Exception as e:
        print(f"Failed to upload {file_name} to S3: {e}")


def append_to_fetch_log(city, source, pm25_val):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = {"timestamp": timestamp, "city": city, "source": source, "pm2_5": pm25_val}
    df = pd.DataFrame([row])
    df.to_csv("fetch_log.csv", mode="a", header=not pd.io.common.file_exists("fetch_log.csv"), index=False)
    upload_to_s3("fetch_log.csv", "aeroc-data-logs")


ph_city_coords = {
    "Manila": (14.6, 120.98), "Quezon City": (14.6760, 121.0437), "Taguig": (14.5176, 121.0509),
    "Makati": (14.5547, 121.0244), "Pasig": (14.5764, 121.0851), "Pasay": (14.5378, 121.0014),
    "Mandaluyong": (14.5794, 121.0359), "Parañaque": (14.4711, 121.0198), "Caloocan": (14.65, 120.97),
    "San Juan": (14.6020, 121.0392), "Las Piñas": (14.4500, 120.9833), "Marikina": (14.6507, 121.1029),
    "Valenzuela": (14.7011, 120.9830)
}

id_city_coords = {
    "Jakarta": (-6.2088, 106.8456), "Surabaya": (-7.2504, 112.7688), "Bandung": (-6.9175, 107.6191),
    "Medan": (3.5952, 98.6722), "Semarang": (-6.9667, 110.4167), "Makassar": (-5.1477, 119.4327),
    "Depok": (-6.4025, 106.7942), "Tangerang": (-6.1783, 106.6319)
}


def fetch_from_open_meteo(lat, lon, city):
    url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={lat}&longitude={lon}&hourly=pm2_5"
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        pm25_val = round(resp.json()["hourly"]["pm2_5"][0], 2)
        append_to_fetch_log(city, "Open-Meteo", pm25_val)
        return pm25_val
    except Exception as e:
        print(f"Open-Meteo Error ({lat}, {lon}): {e}")
        return None


def run_simulation(duration_minutes, selected_country, selected_city, selected_activity):
    all_data = []

    for city, (lat, lon) in ph_city_coords.items():
        pm25 = fetch_from_open_meteo(lat, lon, city)
        if pm25 is not None:
            all_data.append({"location_name": city, "value": pm25, "country": "Philippines"})

    for city, (lat, lon) in id_city_coords.items():
        pm25 = fetch_from_open_meteo(lat, lon, city)
        if pm25 is not None:
            all_data.append({"location_name": city, "value": pm25, "country": "Indonesia"})

    df_all = pd.DataFrame(all_data)

    activities = pd.DataFrame({
        "activity": ["resting", "walking", "jogging"],
        "breathing_rate": [0.006, 0.015, 0.035]
    })

    df_all["key"] = 1
    activities["key"] = 1
    combined = pd.merge(df_all, activities, on="key").drop("key", axis=1)

    combined["micrograms_pm25_inhaled"] = (
        combined["value"] * combined["breathing_rate"] * duration_minutes
    ).round(2)

    combined["who_threshold"] = (
        15 * combined["breathing_rate"] * duration_minutes
    ).round(2)

    def classify_risk(row):
        if row["micrograms_pm25_inhaled"] <= row["who_threshold"]:
            return "Low"
        elif row["micrograms_pm25_inhaled"] <= row["who_threshold"] * 2:
            return "Moderate"
        else:
            return "High"

    def health_effects(risk_level):
        if risk_level == "Low":
            return "Minimal impact for most. Sensitive individuals may feel mild irritation."
        elif risk_level == "Moderate":
            return "Possible coughing, throat irritation, and reduced lung function. Sensitive groups at risk."
        else:
            return "Increased risk of coughing, wheezing, and respiratory stress. Everyone may be affected."

    def pm25_to_cigarettes(pm25_ug):
        return round(pm25_ug / 528, 3)

    combined["who_risk_level"] = combined.apply(classify_risk, axis=1)
    combined["health_effects"] = combined["who_risk_level"].apply(health_effects)
    combined["cigarette_equivalence"] = combined["micrograms_pm25_inhaled"].apply(pm25_to_cigarettes)
    combined["time_fetched"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    full_df = combined.rename(columns={"location_name": "city"})
    full_df.to_csv("all_simulated_data.csv", index=False)
    upload_to_s3("all_simulated_data.csv", "aeroc-data-logs")

    ph_city_to_province = {
        "Manila": "NCR", "Quezon City": "NCR", "Taguig": "NCR", "Makati": "NCR",
        "Pasig": "NCR", "Pasay": "NCR", "Mandaluyong": "NCR", "Parañaque": "NCR",
        "Caloocan": "NCR", "San Juan": "NCR", "Las Piñas": "NCR", "Marikina": "NCR",
        "Valenzuela": "NCR"
    }

    id_city_to_province = {
        "Jakarta": "DKI Jakarta", "Surabaya": "East Java", "Bandung": "West Java",
        "Medan": "North Sumatra", "Semarang": "Central Java", "Makassar": "South Sulawesi",
        "Depok": "West Java", "Tangerang": "Banten"
    }

    def map_province(row):
        if row["country"] == "Philippines":
            return ph_city_to_province.get(row["city"], "Unknown")
        elif row["country"] == "Indonesia":
            return id_city_to_province.get(row["city"], "Unknown")
        return "Unknown"

    full_df["province"] = full_df.apply(map_province, axis=1)

    province_summary = full_df.groupby(["country", "province"]).agg({
        "value": "mean",
        "micrograms_pm25_inhaled": "mean"
    }).reset_index().round(2)

    province_summary.to_csv("province_choropleth_data.csv", index=False)
    upload_to_s3("province_choropleth_data.csv", "aeroc-data-logs")

    user_row = full_df[
        (full_df["city"] == selected_city) &
        (full_df["country"] == selected_country) &
        (full_df["activity"] == selected_activity)
    ]

    user_row.to_csv("user_input_simulation.csv", index=False)
    upload_to_s3("user_input_simulation.csv", "aeroc-data-logs")

    return user_row.to_dict("records")[0], full_df


if __name__ == "__main__":
    user_result, full_data = run_simulation(
        duration_minutes=60,
        selected_country="Philippines",
        selected_city="Quezon City",
        selected_activity="jogging"
    )
    print(user_result)
