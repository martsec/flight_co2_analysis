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
st.title('Greenwashing top')


@st.cache
def load_attribution(countries=None, owners=None):
    df = pd.read_csv("export/attribution_co2.csv", header=0)
    if countries:
        df = df[df.country.isin(countries)]
    if owners:
        df = df[df.ownop.isin(owners)]
    return df

@st.cache
def load_flight_data(ownop, icao):
    try:
        df = pd.read_csv(f"export/trips_history/{icao}.csv", header=0)
        df["ownop"] = ownop
        return df.sort_values(["time"])
    except:
        df = pd.DataFrame(columns=["ownop", "lat", "lon", "time"])
        return df


def plot_map(df, color="ownop"):
    fig = px.line_mapbox(df, lat="lat", lon="lon", color="ownop", height=600) #, width=1500, height=800)
    fig.update_layout(mapbox_style="open-street-map", mapbox_zoom=1, mapbox_center_lat = 40, mapbox_center_lon=0,
                      margin={"r":0,"t":0,"l":0,"b":0})
    return fig
    

# Load data
countries=[]
ownop =[]
attribution = load_attribution(countries=countries, owners=ownop)


# Selectors
rank_y_column = st.sidebar.selectbox("Metrics in", [c for c in attribution.columns if c not in ("ownop", "reg", "icao")])
countries = st.sidebar.multiselect("Country", attribution.country.unique(), default=None)
ownop = st.sidebar.multiselect("Choose polluters", attribution.ownop.unique(), default=None)
attribution = load_attribution(countries=countries, owners=ownop)

from datetime import datetime, timezone, timedelta
yesterday = datetime.now(timezone.utc) - timedelta(days=1)
one_week_ago = datetime.now(timezone.utc) - timedelta(days=45)
time_range = (one_week_ago, yesterday)
time_range = st.sidebar.slider(
    "Time range:",
    value=time_range,
    format="DD/MM/YY",
)
st.sidebar.write("time range", time_range)

st.subheader('Most contaminating billionares')
#countries = st.multiselect("Choose country", attribution.country, default=None) 



#st.bar_chart(attribution, x="ownop", y=rank_y_column)

# Horizontal stacked bar chart
chart = (
    alt.Chart(attribution.sort_values("co2_tons")[:20])
    .mark_bar()
    .encode(
        x=alt.X(rank_y_column, type="quantitative", title=""),
        y=alt.Y("ownop", type="nominal", title=""),
    )
)

st.bar_chart(attribution.sort_values("co2_tons")[:20], x="ownop", y=rank_y_column)

st.altair_chart(chart, use_container_width=True)

st.subheader('Trip map')
col1, col2, col3 = st.columns(3)
col1.metric("Flown Hours", np.round(attribution.air_h.sum(), 2))
col2.metric("CO2 tons", np.round(attribution.co2_tons.sum(), 2), "+8%")
col3.metric("Years of an european", np.round(attribution["times_European Union (28)_yr"].sum(), 2), "4%")
    
if len(ownop) == 0 and not countries:
    flight_data = pd.DataFrame(columns=["ownop", "lat", "lon"])
    st.write('Too much data to show a map')
elif not ownop and countries:
    pairs = load_attribution(countries=countries, owners=ownop)[["ownop", "icao"]].values.tolist()
    flight_data = pd.concat([load_flight_data(p[0], p[1]) for p in pairs])
    st.plotly_chart(plot_map(flight_data) , use_container_width=True)

else:
    pairs = load_attribution(countries=countries, owners=ownop)[["ownop", "icao"]].values.tolist()
    flight_data = pd.concat([load_flight_data(p[0], p[1]) for p in pairs])
    st.plotly_chart(plot_map(flight_data) , use_container_width=True)
