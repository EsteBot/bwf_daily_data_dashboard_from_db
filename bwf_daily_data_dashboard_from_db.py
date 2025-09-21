import streamlit as st
import altair as alt
import pandas as pd
import sqlite3
import os
from datetime import date, timedelta

# --- Configuration ---
DB_FILE_NAME = "hotel_data.db"
TABLE_NAME = "daily_hourly_metrics"
MAX_HOTEL_CAPACITY = 60 # Your specified maximum rooms the hotel can fill

# --- Function to load ALL data from SQLite (with caching for display/other uses) ---
@st.cache_data
def load_all_data(db_path, table_name):
    """Loads all data from SQLite database into a Pandas DataFrame."""
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()

        # Ensure DateTime is correctly parsed (it might be string from DB)
        df['DateTime'] = pd.to_datetime(df['DateTime'])

        # Ensure numerical columns are truly numeric (fillna(0) handles 'sold out' converted to NaN)
        numerical_cols = ['Rooms Sold', 'Rooms Available', 'Arrivals', 'OOO Rooms', 'King Rate', 'QQ Rate']
        for col in numerical_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        st.info(f"Please ensure '{DB_FILE_NAME}' exists in the same directory as this app, and run the sync script.")
        return pd.DataFrame() # Return empty DataFrame on error

# --- Function to get Overall Occupancy Rate using SQL ---
@st.cache_data
def get_overall_occupancy_rate(db_path, table_name, max_capacity, start_date, end_date):
    """
    Calculates the overall average occupancy rate based on 21:00 Rooms Sold entries within a date range.
    """
    try:
        conn = sqlite3.connect(db_path)
        sql_query = f"""
        SELECT
            AVG(CAST("Rooms Sold" AS REAL) * 100.0 / {max_capacity}) AS avg_daily_2100_occupancy
        FROM
            {table_name}
        WHERE
            STRFTIME('%H:%M', DateTime) = '21:00' AND
            DATE(DateTime) BETWEEN '{start_date}' AND '{end_date}';
        """
        result_df = pd.read_sql_query(sql_query, conn)
        conn.close()
        occupancy_rate = result_df['avg_daily_2100_occupancy'].iloc[0] if not result_df.empty else 0.0
        return occupancy_rate
    except Exception as e:
        st.error(f"Error calculating occupancy rate: {e}")
        return 0.0 

# --- Function to get Total OOO Rooms at 21:00 using SQL ---
@st.cache_data
def get_total_ooo_rooms_at_2100(db_path, table_name, start_date, end_date):
    """
    Calculates the total sum of 'OOO Rooms' specifically at 21:00 entries within a date range.
    """
    try:
        conn = sqlite3.connect(db_path)
        sql_query = f"""
        SELECT
            SUM("OOO Rooms") AS total_ooo_at_2100
        FROM
            {table_name}
        WHERE
            STRFTIME('%H:%M', DateTime) = '21:00' AND
            DATE(DateTime) BETWEEN '{start_date}' AND '{end_date}';
        """
        result_df = pd.read_sql_query(sql_query, conn)
        conn.close()
        total_ooo = result_df['total_ooo_at_2100'].iloc[0] if not result_df.empty else 0
        return total_ooo
    except Exception as e:
        st.error(f"Error calculating total OOO rooms: {e}")
        return 0

# --- Streamlit App Layout ---

st.set_page_config(layout="wide", page_title="Hotel Performance Dashboard")

st.markdown("""
<style>
.center { display: flex; justify-content: center; text-align: center; }
</style>
""", unsafe_allow_html=True)

st.markdown("<h2 class='center' style='color:rgb(70, 130, 255);'>An EsteStyle Streamlit Page<br>Where Python Wiz Meets Data Biz!</h2>", unsafe_allow_html=True)
st.markdown("<img src='https://1drv.ms/i/s!ArWyPNkF5S-foZspwsary83MhqEWiA?embed=1&width=307&height=307' width='300' style='display: block; margin: 0 auto;'>", unsafe_allow_html=True)
st.markdown("<h3 class='center' style='color: rgb(135, 206, 250);'>🏨 Originally created for Best Western at Firestone 🛎️</h3>", unsafe_allow_html=True)
st.markdown("<h3 class='center' style='color: rgb(135, 206, 250);'>🤖 By Esteban C Loetz 📟</h3>", unsafe_allow_html=True)
st.markdown("##")
st.markdown("---")
st.markdown("<h3 class='center' style='color: rgb(112, 128, 140);'>📄 Hotel Performance Overview 🖋️</h3>", unsafe_allow_html=True)

# Sidebar widgets
graph_type = st.sidebar.selectbox("Select Graph Type", ["Line", "Bar"])
st.sidebar.markdown("<br>", unsafe_allow_html=True)
st.sidebar.markdown("<br>", unsafe_allow_html=True)
st.sidebar.markdown("<br>", unsafe_allow_html=True)
time_scale = st.sidebar.selectbox("Select Time Scale", ["Month", "Week", "Day"])
st.sidebar.markdown("<br>", unsafe_allow_html=True)
st.sidebar.markdown("<br>", unsafe_allow_html=True)
st.sidebar.markdown("<br>", unsafe_allow_html=True)
st.sidebar.markdown("<br>", unsafe_allow_html=True)
st.sidebar.markdown("<br>", unsafe_allow_html=True)

# Load all data (for general use or preview, not directly for the specific occupancy metric)
all_data = load_all_data(DB_FILE_NAME, TABLE_NAME)

# Get the min and max dates from the full dataset
min_date = all_data['DateTime'].min().date() if not all_data.empty else date.today()
max_date = all_data['DateTime'].max().date() if not all_data.empty else date.today()

# --- Sidebar Controls ---
with st.sidebar:
    date_range = st.date_input(
        "Select a Date Range",
        [min_date, max_date], # Default to the full range
        min_value=min_date,
        max_value=max_date
    )
    st.info("Metrics will update based on this date range.")
    
# Make sure a valid date range was selected
if len(date_range) == 2:
    start_date, end_date = date_range[0], date_range[1]
else:
    # Handle the case where only one date is selected
    start_date, end_date = date_range[0], date_range[0]
    
# Filter the DataFrame based on the user's selection
filtered_data = all_data[(all_data['DateTime'].dt.date >= start_date) & (all_data['DateTime'].dt.date <= end_date)]


if not filtered_data.empty:

    # Map time_scale to resample frequency
    freq_map = {
        "Day": "D",
        "Week": "W-MON",
        "Month": "ME"
    }
    resample_freq = freq_map[time_scale]

    # Resample and format
    def resample_data(df, freq):
        return df.resample(freq).mean()

    def format_labels(df, time_scale):
        if time_scale == "Day":
            df['Label'] = df.index.strftime('%b %d')
        elif time_scale == "Week":
            df['Label'] = df.index.strftime('Week of %b %d')
        elif time_scale == "Month":
            df['Label'] = df.index.strftime('%B')
        return df


    def plot_chart(df, graph_type):
        if graph_type == "Bar":
            chart = alt.Chart(df).mark_bar().encode(
                x=alt.X('Label:N', title='Date'),
                y=alt.Y('OOO Rooms', title='Out-of-Order Rooms'),
                tooltip=['Label', 'OOO Rooms']
            )
        else:
            chart = alt.Chart(df).mark_line().encode(
                x=alt.X('Label:N', title='Date'),
                y=alt.Y('OOO Rooms', title='Out-of-Order Rooms'),
                tooltip=['Label', 'OOO Rooms']
            )
        return chart.properties(width=800, height=400)


    st.subheader("OOO Rooms Trend")

    # Filter to 9 PM data
    ooo_data_base = filtered_data[filtered_data['DateTime'].dt.hour == 21].set_index('DateTime')

    if not ooo_data_base.empty:
        resampled = resample_data(ooo_data_base, resample_freq)
        labeled = format_labels(resampled, time_scale)
        chart = plot_chart(labeled, graph_type)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No OOO Rooms data available for the selected date range.")

    '''
    # --- Daily Occupancy Rate Chart ---
    st.subheader(f"Daily Occupancy Rate Trend by {time_scale}")
    # Calculate occupancy rate per day: (Rooms Sold at 21:00 / MAX_HOTEL_CAPACITY) * 100
        
    if not occ_data_base.empty:
        occ_data_base['Occupancy Rate'] = occ_data_base['Rooms Sold'] * 100.0 / MAX_HOTEL_CAPACITY
        if time_scale == "Month":
            occ_chart_data = occ_data_base['Occupancy Rate'].resample('ME').mean()
        elif time_scale == "Week":
            occ_chart_data = occ_data_base['Occupancy Rate'].resample('W').mean()
        else:
            occ_chart_data = occ_data_base['Occupancy Rate'].resample('D').mean()
        if graph_type == "Line":
            st.line_chart(occ_chart_data, use_container_width=True)
        elif graph_type == "Bar":
            st.bar_chart(occ_chart_data, use_container_width=True)
    else:
        st.info("No occupancy rate data for the selected date range.")

    # --- Daily Arrivals at 3 PM Chart ---
    st.subheader(f"Daily Arrivals at 3 PM Trend by {time_scale}")
    
    if not arrivals_3pm_base.empty:
        if time_scale == "Month":
            arrivals_chart_data = arrivals_3pm_base['Arrivals'].resample('ME').mean()
        elif time_scale == "Week":
            arrivals_chart_data = arrivals_3pm_base['Arrivals'].resample('W').mean()
        else:
            arrivals_chart_data = arrivals_3pm_base['Arrivals'].resample('D').mean()
        if graph_type == "Line":
            st.line_chart(arrivals_chart_data, use_container_width=True)
        elif graph_type == "Bar":
            st.bar_chart(arrivals_chart_data, use_container_width=True)
    else:
        st.info("No arrivals at 3 PM data for the selected date range.")'''
    # --- Calculate Metrics on the FILTERED Data ---
    occupancy_rate = get_overall_occupancy_rate(DB_FILE_NAME, TABLE_NAME, MAX_HOTEL_CAPACITY, start_date, end_date)
    avg_king_rate = filtered_data['King Rate'].mean()
    avg_qq_rate = filtered_data['QQ Rate'].mean()
    arrivals_3pm_data = filtered_data[filtered_data['DateTime'].dt.hour == 15]['Arrivals']
    avg_arrivals_3pm = arrivals_3pm_data.mean() if not arrivals_3pm_data.empty else 0
    total_ooo_rooms = get_total_ooo_rooms_at_2100(DB_FILE_NAME, TABLE_NAME, start_date, end_date)
    total_days_in_db = filtered_data['DateTime'].dt.date.nunique()

    # --- Display Metrics using st.metric ---
    st.header("Overall Key Performance Indicators")

    col1, col2, col3, col4, col5 = st.columns(5) 

    with col1:
        st.metric(label=f"Avg. Daily Occupancy\n@ 9 PM", value=f"{occupancy_rate:.1f}%")
    with col2:
        st.metric(label="Avg. Daily Arrivals\n@ 3 PM", value=f"{avg_arrivals_3pm:.1f}")
    with col3:
        st.metric(label=f"Total OOO Rooms \n@ 9 PM (out of {total_days_in_db} days)", value=f"{int(total_ooo_rooms)}")
    with col4:
        st.metric(label="Avg. King Rate", value=f"${avg_king_rate:.2f}")
    with col5:
        st.metric(label="Avg. QQ Rate", value=f"${avg_qq_rate:.2f}")

    st.markdown("---") 

    st.subheader("Raw Data Preview")
    st.dataframe(all_data.tail(6)) 

else:
    st.warning("No data loaded. Please check your database file and sync script.")