import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import plotly.express as px
import s3fs

st.set_page_config(
    page_title="Greenwashing Flights",
    page_icon="🌏",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://www.extremelycoolapp.com/help',
        'Report a bug': "https://www.extremelycoolapp.com/bug",
        'About': "# This is a header. This is an *extremely* cool app!"
    }
)
st.title('Greenwashing Ranking')


with st.expander("Los Nadies - Eduardo Galeano"):
    # Motivation
    mot1, mot2 = st.columns(2)
    mot1.write("""Sueñan las pulgas con comprarse un perro   
    y sueñan los nadies con salir de pobres,   
    que algún mágico día   
    llueva de pronto la buena suerte,   
    que llueva a cántaros la buena suerte;   
    pero la buena suerte no llueve ayer,   
    ni hoy, ni mañana, ni nunca,   
    ni en lloviznita cae del cielo la buena suerte,  
    por mucho que los nadies la llamen  
    y aunque les pique la mano izquierda,  
    o se levanten con el pie derecho,  
    o empiecen el año cambiando de escoba.  
    Los nadies: los hijos de nadie, los dueños de nada.  
    Los nadies: los ningunos, los ninguneados,  
    corriendo la liebre, muriendo la vida, jodidos, rejodidos:  
    """)
    mot2.write("""Que no son, aunque sean.   
    Que no hablan idiomas, sino dialectos.  
    Que no profesan religiones, sino supersticiones.  
    Que no hacen arte, sino artesanía.  
    Que no practican cultura, sino folclore.  
    Que no son seres humanos, sino recursos humanos.  
    Que no tienen cara, sino brazos.  
    Que no tienen nombre, sino número.  
    Que no figuran en la historia universal,  
    sino en la crónica roja de la prensa local.  
    Los nadies que cuestan menos que la bala que los mata.  
    """)

if st.secrets["env"] == "streamlit":
    storage_options = {
        "anon": False,
        "key": st.secrets["AWS_SECRET_KEY_ID"],
        "secret": st.secrets["AWS_SECRET_ACCESS_KEY"],
        "client_kwargs": {'endpoint_url': st.secrets["AWS_S3_ENDPOINT"]}
    }
    fs = s3fs.S3FileSystem(**storage_options)

    @st.experimental_memo(ttl=600)
    def load_attribution(countries=None, owners=None):
        with fs.open("s3://gwt/attribution_co2.csv") as f:
            df = pd.read_csv(f, header=0)
        if countries:
            df = df[df.country.isin(countries)]
        if owners:
            df = df[df.ownop.isin(owners)]
        return df
    
    @st.experimental_memo(ttl=600)
    def load_flight_icao(ownop, icao):
        try:
            with fs.open(f"s3://gwt/trips_history/{icao}.csv") as f:
                df = pd.read_csv(f, header=0)
                df["ownop"] = ownop
            return df.sort_values(["time"])
        except:
            df = pd.DataFrame(columns=["ownop", "lat", "lon", "time"])
            return df

    @st.experimental_memo(ttl=600)
    def load_co2_country():
        with fs.open("s3://gwt/co2_per_country.csv") as f:
            df = pd.read_csv(f, header=0)
        return df
    



else:
    #@st.cache
    def load_attribution(countries=None, owners=None):
        df = pd.read_csv("export/attribution_co2.csv", header=0)
        if countries:
            df = df[df.country.isin(countries)]
        if owners:
            df = df[df.ownop.isin(owners)]
        return df
    
    #@st.cache
    def load_flight_icao(ownop, icao):
        try:
            df = pd.read_csv(f"export/trips_history/{icao}.csv", header=0)
            df["ownop"] = ownop
            return df.sort_values(["time"])
        except:
            df = pd.DataFrame(columns=["ownop", "lat", "lon", "time"])
            return df
        
    def load_co2_country():
        return pd.read_csv("export/co2_per_country.csv", header=0)
    

def load_flight_data(pairs):
    # my_bar = st.progress(0)
    # percent_complete = 1 / len(pairs)
    # acc = 0
    # pds = []
    # for p in pairs:
    #     ownop = p[0]
    #     icao = p[1]
    #     try:
    #         with fs.open(f"s3://gwt/trips_history/{icao}.csv") as f:
    #             df = pd.read_csv(f, header=0)
    #             df["ownop"] = ownop
    #     except:
    #         df = pd.DataFrame(columns=["ownop", "lat", "lon", "time"])
    #     pds.append(df.sort_values(["time"]))
    #     my_bar.progress(percent_complete)
    #     acc += percent_complete

    flight_data = pd.concat([load_flight_icao(p[0], p[1]) for p in pairs])
    #flight_data = pd.concat(pds)

    #my_bar.progress(1 - acc)
    return flight_data

def plot_map(df, color="ownop"):
    fig = px.line_mapbox(df, lat="lat", lon="lon", color="ownop", height=600) #, width=1500, height=800)
    fig.update_layout(mapbox_style="open-street-map", mapbox_zoom=1, mapbox_center_lat = 40, mapbox_center_lon=0,
                      margin={"r":0,"t":0,"l":0,"b":0})
    return fig

def horizontal_bar(df, x, y):
    grouped = attribution[[x, y]].groupby(x).sum().reset_index()
    chart = (
        alt.Chart(grouped.sort_values(y, ascending=False)[:20])
        .mark_bar()
        .encode(
            x=alt.X(y, type="quantitative", title=""),
            y=alt.Y(x, type="nominal", title=""),
        )
    )
    return chart

# Load data
countries=[]
ownop =[]
attribution = load_attribution(countries=countries, owners=ownop)


TIPS = [
    "Look for Aircraft 'EMS-2', now owned by the _Jordan Royal Squadron_ that was probably a spanish aircraft, since EM is reserved for the country's militar aircarfts.",
    "Look for 'Iron Maiden' to see all the places they have been performing at.",
    "Liechestein and Switzerland have the same registration prefix: 'HB'. So you'll get data from both together.",
    "There is data from individuals and companies owning 2 jets or less.",
    "Fuel consumed and CO2 generated is an approximation and is probably higher than what is stated here. It depends on plane weight, speed, flight route...",
    "Ownership for non-US jets is not as good as we would like.",
    "Improvement plan: Detect if owner is a company or an individual with a Named Entity Recognition algorithm (bert-base-NER from hugginface)",
    "Improvement plan: Allow selecting multiple days at once.",
    "Improvement plan: form to improve ownership data."
]
SOURCES = [
    "[CO2 Data explorer](https://ourworldindata.org/explorers/co2) - World of Data",
    "[Countries with Regional Codes](https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes) - Lukes CC BY SA",
    "[Registration country code prefix](https://en.wikipedia.org/wiki/List_of_aircraft_registration_prefixes) - Wikipedia",
    "[ADS-B Exchange](https://www.adsbexchange.com/) - Flight paths and aircraft ownership (collaborative)",
    "[Open Sky Network](https://opensky-network.org/) - Flight paths and aircraft ownership (collaborative)",
    "[Private Jet Fuel Burn](https://compareprivateplanes.com/articles/private-jet-fuel-burn-per-hour) - compareprivateplanes",
    "[Aircraft Type Fuel Consumptions Rate](https://github.com/Jxck-S/plane-notify/blob/multi/aircraft_type_fuel_consumption_rates.json) - Jack Sweeney Plane notify",
]
import random


st.info(random.choice(TIPS), icon="ℹ️")
co2_countries = load_co2_country()

# Selectors
st.sidebar.write(
    "> In a world with dark and gloomy future, _los nadies_ are requested to use less the car, use ventilators instead of ACs, fly less, etc. while those tho think are someone contribure more everytime to the global warming.")


st.sidebar.subheader("Instructions")

country_options = co2_countries.country.to_list()
country_comparison = st.sidebar.selectbox("Country CO2 per capita to compare to", country_options, index=country_options.index("World"))
co2_per_capita = float(co2_countries.loc[co2_countries.country == country_comparison, "co2_per_capita"].values[0])

# rank_y_column = st.sidebar.selectbox("Metrics in", [c for c in attribution.columns if c not in ("ownop", "reg", "icao")])

countries = st.sidebar.multiselect("Jet owners country", attribution.country.unique(), default=None)
ownop = st.sidebar.multiselect("Filter Jet owners", attribution.ownop.unique(), default=None)
date = st.sidebar.selectbox("Choose Date", attribution.date.unique())

st.sidebar.write("Filter by Jet owners or countries to show the map.")
st.sidebar.empty()
st.sidebar.subheader("Sources")
st.sidebar.caption("  \n".join(SOURCES))



attribution = load_attribution(countries=countries, owners=ownop)
attribution = attribution[attribution.date == date]
hourly_co2_capita = co2_per_capita / (365 * 24)
attribution["metric"] = attribution["co2_tons"] / (attribution["air_h"] * hourly_co2_capita)

#from datetime import datetime, timezone, timedelta
#yesterday = datetime.now(timezone.utc) - timedelta(days=1)
#one_week_ago = datetime.now(timezone.utc) - timedelta(days=45)
#time_range = (one_week_ago, yesterday)
#time_range = st.sidebar.slider(
#    "Time range:",
#    value=time_range,
#    format="DD/MM/YY",
#)
#st.sidebar.write("time range", time_range)

num_owners = attribution.ownop.nunique()
num_hrs = np.round(attribution.air_h.sum(), 2)
num_co2 = np.round(attribution.co2_tons.sum(), 2)
num_citizens = int(attribution["metric"].sum())

st.error(
    f"On {date}, {num_owners} jet owners polluted at least like "
    f"{num_citizens} {country_comparison} citizens. "
    f"They flew {num_hrs} hours and generated {num_co2} CO2 Tons."
    f"For comparison, a citizen from {country_comparison} generates "
    f"{np.round(co2_per_capita, 2)} CO2 Tons **per year**.",
    icon="🔥"
)

col1, col2, col3 = st.columns(3)
col1.metric("Flown Hours", num_hrs)
col2.metric("CO2 tons", num_co2) #, "+8%")
col3.metric("equivalent to", num_citizens, f" {country_comparison} citizens", delta_color="inverse")
#countries = st.multiselect("Choose country", attribution.country, default=None) 



#st.bar_chart(attribution, x="ownop", y=rank_y_column)

# Horizontal stacked bar chart
col1, col2 = st.columns(2)

with col1:
    st.subheader("Owners rank")
    st.altair_chart(horizontal_bar(attribution, "ownop", "metric"), use_container_width=True)

with col2:
    st.subheader("Country rank")
    st.altair_chart(horizontal_bar(attribution, "country", "metric"), use_container_width=True)



st.subheader('Trip map')

num_owners, num_planes, jetw_with_co2= st.columns(3)
num_planes.metric("Unique Jets", attribution.icao.nunique())
num_owners.metric("Unique Owners", attribution.ownop.nunique())
jetw_with_co2.metric("Jets With CO2 data", attribution.co2_tons.count())

if len(ownop) == 0 and not countries:
    flight_data = pd.DataFrame(columns=["ownop", "lat", "lon"])
    st.write('Too much data to show a map')
else:
    pairs = load_attribution(countries=countries, owners=ownop)[["ownop", "icao"]].drop_duplicates().values.tolist()
    flight_data = load_flight_data(pairs)
    flight_data = flight_data[flight_data.date == date]
    if not flight_data.empty:
        st.plotly_chart(plot_map(flight_data) , use_container_width=True)
