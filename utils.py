from pyspark.sql.window import Window
import pandas as pd
import plotly.express as px
from pyspark.sql.functions import *


def toPd(df, limit=100):
    return df.limit(limit).toPandas()

def compute_flight_time(ticks, s_between_ticks = 5):
    """ Calculates flight time in seconds and hours 
    Requires a dataset with the fields `icao` (unique icao hex code)
    
    """
    flight_time = ticks.where("altitude_baro > 10").\
      groupBy("icao").count().\
      withColumns({
        "air_s": (col("count") + 40) * s_between_ticks,  
        "air_h": (col("count") + 40) * s_between_ticks / 3600, 
      }).\
      drop("count")
    return flight_time

def compute_co2(flight_time, fuel_consumption, aircrafts):
    with_type = flight_time.join(aircrafts.select("icao", "icaotype"), on="icao", how="left")
    
    return with_type\
      .join(fuel_consumption.select(["icaotype", "galph"]) , on="icaotype")\
      .withColumn("fuel_used_kg", col("air_s")/3600 * col("galph") * 3.04)\
      .withColumn("co2_tons", col("fuel_used_kg") * 3.15 / 907.185)\
      .drop("icaotype")

def attribute_co2(co2_generated, aircraft_ownership):
    return co2_generated\
      .join(aircraft_ownership.select("icao", "ownop") , on="icao")

def tick_to_attribution(aircraft_ticks, fuel_consumption, aircraft_ownership, s_between_ticks = 5):
    ft = compute_flight_time(aircraft_ticks, s_between_ticks)
    co2 = compute_co2(ft, fuel_consumption, aircraft_ownership)
    return attribute_co2(co2, aircraft_ownership)

def get_individually_owned(trips, aircrafts_db): 
    # Filtering by owners
    filtered_aircrafts = get_individually_owned_icao(aircrafts_db)
    return trips.join(filtered_aircrafts, on="icao")

def get_individually_owned_icao(aircrafts_db): 
    individuals = aircrafts_db.groupby("ownop").count().where("count <= 2").drop("count")
    common_airline_words = [
        "air", "charter", "trust", "llc", "bank", "corp", "industries" "inc", "leasing", "properties", "holding", "group", "police", "service", "govern",
        "pending", "jet", "aviation", "swoop", "limited", "state", "minist", "governmen", "ltd", "fund", "department", "sidney", "foundation"
    ]
    end_filter = ["co", "builders", "farms", "lp", "city"]
    for word in common_airline_words:
        individuals = individuals.where(f"LOWER(ownop) not like '%{word}%'")
    for word in end_filter:
        individuals = individuals.where(f"LOWER(ownop) not like '%{word}'")
    return individuals.join(aircrafts_db.select("ownop", "icao"), on="ownop").select("icao")

def resample(ticks, unix_s_col, plane_identifier="icao", sampling_s=60):
    resampled_col = f"{unix_s_col}_resampled"
    buckets = (col(unix_s_col) / sampling_s).cast("bigint") * sampling_s
    resampled_window = Window.partitionBy(plane_identifier, resampled_col).orderBy(unix_s_col)

    ts_resampled = ticks.withColumn(resampled_col, buckets)
    # Needed to avoid too many files open exceptions
    ts_resampled.cache()

    resampled = ts_resampled.\
      withColumn("_window_rank", row_number().over(resampled_window)).\
      where("_window_rank = 1").drop("_window_rank")
    return resampled



def get_per_capita_emissions(aircraft_co2, country_co2_emissions):
    co2_per_capita = country_co2_emissions.select("iso_code", "country", "year", "co2_per_capita", "energy_per_capita", "nitrous_oxide_per_capita")\
      .where("year == 2019")
    countries = ["European Union (28)", "Africa", "Argentina", "United Kingdom", "United States", "Australia", "China", "India"]
    co2_capita_selection = {}
    for c in countries:
        co2 = co2_per_capita.where(f"country like '{c}'").head().asDict().get("co2_per_capita")
        co2_capita_selection[f"times_{c}_yr"] = col("co2_tons")/co2
    co2_with_capita = aircraft_co2.select("ownop", "icao", "air_h", "co2_tons")\
      .withColumns(co2_capita_selection).orderBy(col("co2_tons").desc())
    return co2_with_capita

