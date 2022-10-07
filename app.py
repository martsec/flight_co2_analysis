import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import plotly.express as px
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
    st.write("> In a world were the future is darker, los nadies are requested to use less the car, use ventilators instead of ACs, fly less, etc. while those tho think are someone contribure more everytime to the global warming.")

st.title('Greenwashing top')

if st.secrets["env"] == "streamlit:
    @st.experimental_memo(ttl=600)
    def load_attribution(countries=None, owners=None):
        df = pd.read_csv("s3://gwt/export/attribution_co2.csv", header=0)
        if countries:
            df = df[df.country.isin(countries)]
        if owners:
            df = df[df.ownop.isin(owners)]
        return df
    
    @st.experimental_memo(ttl=600)
    def load_flight_data(ownop, icao):
        try:
            df = pd.read_csv(f"s3://gwt/export/trips_history/{icao}.csv", header=0)
            df["ownop"] = ownop
            return df.sort_values(["time"])
        except:
            df = pd.DataFrame(columns=["ownop", "lat", "lon", "time"])
            return df

    @st.experimental_memo(ttl=600)    
    def load_co2_country():
        return pd.read_csv("s3://gwt/export/co2_per_country.csv", header=0)
    



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
    def load_flight_data(ownop, icao):
        try:
            df = pd.read_csv(f"export/trips_history/{icao}.csv", header=0)
            df["ownop"] = ownop
            return df.sort_values(["time"])
        except:
            df = pd.DataFrame(columns=["ownop", "lat", "lon", "time"])
            return df
        
    def load_co2_country():
        return pd.read_csv("export/co2_per_country.csv", header=0)
    

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
    "Look for Aircraft 'EMS-2', now owned by the Jordan Royal Squadron that was probably a spanish aircraft, since EM is reserved for the country's militar aircarfts.",
    "Look for 'Iron Maiden' to see all the places they have been performing at.",
    "Liechestein and Switzerland have the same registration prefix: 'HB'. So you'll get data from both together.",
    "There is data from individuals and companies owning 2 jets or less.",
    "Fuel consumed and CO2 generated is an approximation and is probably higher than what is stated here. It depends on plane weight, speed, flight route...",
]
SOURCES = [
 "Source: [CO2 Data explorer](https://ourworldindata.org/explorers/co2) - World of Data",   
]
import random

st.info(random.choice(TIPS + SOURCES), icon="ℹ️")
co2_countries = load_co2_country()

# Selectors
country_options = co2_countries.country.to_list()
country_comparison = st.sidebar.selectbox("Metrics in", country_options, index=country_options.index("World"))
co2_per_capita = float(co2_countries.loc[co2_countries.country == country_comparison, "co2_per_capita"].values[0])

# rank_y_column = st.sidebar.selectbox("Metrics in", [c for c in attribution.columns if c not in ("ownop", "reg", "icao")])

countries = st.sidebar.multiselect("Polluters country", attribution.country.unique(), default=None)
ownop = st.sidebar.multiselect("Choose polluters", attribution.ownop.unique(), default=None)
date = st.sidebar.selectbox("Choose Date", attribution.date.unique())

attribution = load_attribution(countries=countries, owners=ownop)
attribution = attribution[attribution.date == date]
attribution["metric"] = attribution["co2_tons"] * 365 / co2_per_capita

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



col1, col2, col3 = st.columns(3)
col1.metric("Flown Hours", np.round(attribution.air_h.sum(), 2))
col2.metric("CO2 tons", np.round(attribution.co2_tons.sum(), 2)) #, "+8%")
col3.metric("# of citizens", np.round(attribution["metric"].sum(), 0))
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
    pairs = load_attribution(countries=countries, owners=ownop)[["ownop", "icao"]].values.tolist()
    flight_data = pd.concat([load_flight_data(p[0], p[1]) for p in pairs])
    flight_data = flight_data[flight_data.date == date]
    if not flight_data.empty:
        st.plotly_chart(plot_map(flight_data) , use_container_width=True)
