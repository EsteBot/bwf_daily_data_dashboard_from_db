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

        # Ensure DateTime is correctly parsed
        df['DateTime'] = pd.to_datetime(df['DateTime'])

        # Clean truly numeric columns only
        numeric_cols = ['Rooms Sold', 'Rooms Available', 'Arrivals', 'OOO Rooms']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        st.info(f"Please ensure '{DB_FILE_NAME}' exists in the same directory as this app, and run the sync script.")
        return pd.DataFrame()

    
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
filtered_data = all_data[
    (all_data['DateTime'].dt.date >= start_date) &
    (all_data['DateTime'].dt.date <= end_date)
].copy()

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
        # Only resample numeric columns
        numeric_df = df.select_dtypes(include='number')
        return numeric_df.resample(freq).mean()


    def format_labels(df, time_scale):
        if time_scale == "Day":
            df['Label'] = df.index.strftime('%b %d')
        elif time_scale == "Week":
            df['Label'] = df.index.strftime('Week of %b %d')
        elif time_scale == "Month":
            df['Label'] = df.index.strftime('%B')
        return df

    def plot_metric_chart(df, metric_col, hour, title, y_title, graph_type, time_scale):
        df_base = df[df['DateTime'].dt.hour == hour].set_index('DateTime')
        if df_base.empty:
            st.info(f"No {title.lower()} data for the selected date range.")
            return

        resampled = resample_data(df_base, freq_map[time_scale])
        labeled = format_labels(resampled, time_scale)
        labeled[metric_col] = labeled[metric_col].round(1)

        chart = alt.Chart(labeled).mark_line() if graph_type == "Line" else alt.Chart(labeled).mark_bar()
        chart = chart.encode(
            x=alt.X('Label:N', title='Date'),
            y=alt.Y(f'{metric_col}:Q', title=y_title),
            tooltip=['Label', metric_col]
        ).properties(width=800, height=400)

        st.subheader(title)
        st.altair_chart(chart, use_container_width=True)

    # --- Calculate Metrics on the FILTERED Data ---
    occupancy_rate = get_overall_occupancy_rate(DB_FILE_NAME, TABLE_NAME, MAX_HOTEL_CAPACITY, start_date, end_date)

    # Clean King and QQ rates before averaging
    filtered_data['King Rate Clean'] = pd.to_numeric(filtered_data['King Rate'], errors='coerce')
    filtered_data['QQ Rate Clean'] = pd.to_numeric(filtered_data['QQ Rate'], errors='coerce')

    avg_king_rate = filtered_data['King Rate Clean'].dropna().mean()
    avg_qq_rate = filtered_data['QQ Rate Clean'].dropna().mean()

    nine_pm_data = filtered_data[filtered_data['DateTime'].dt.hour == 21]

    num_king_sold_out = nine_pm_data['King Rate'].astype(str).str.lower().str.contains('sold out', na=False).sum()
    num_qq_sold_out = nine_pm_data['QQ Rate'].astype(str).str.lower().str.contains('sold out', na=False).sum()

    # Arrivals at 3 PM
    arrivals_3pm_data = filtered_data[filtered_data['DateTime'].dt.hour == 15]['Arrivals']
    avg_arrivals_3pm = arrivals_3pm_data.mean() if not arrivals_3pm_data.empty else 0

    # Total OOO Rooms and total days
    total_ooo_rooms = get_total_ooo_rooms_at_2100(DB_FILE_NAME, TABLE_NAME, start_date, end_date)
    total_days_in_db = filtered_data['DateTime'].dt.date.nunique()

    nine_pm_data = filtered_data[filtered_data['DateTime'].dt.hour == 21]
    ooo_days = nine_pm_data[nine_pm_data['OOO Rooms'] > 0]['DateTime'].dt.date.nunique()
    ooo_day_percent = (ooo_days / total_days_in_db) * 100 if total_days_in_db > 0 else 0

    # --- Display Metrics using st.metric ---
    st.markdown("<h2 style='color:rgb(70, 130, 255); text-align:center;'>Key Performance Indicators for Selected Date</h2>", unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4) 

    st.subheader("📊 Occupancy & Arrivals")
    col1, col2, col3, col4 = st.columns(4) 
    with col2:
        st.metric("📈 Avg. Daily Occupancy @ 9 PM", f"{occupancy_rate:.1f}%")
    with col3:
        st.metric("🚪 Avg. Daily Arrivals @ 3 PM", f"{avg_arrivals_3pm:.1f}")

    st.subheader("💸 Rate Performance")
    col3, col4 = st.columns(2)
    with col3:
        st.metric("💰 Avg. King Rate", f"${avg_king_rate:.2f}")
    with col4:
        st.metric("💵 Avg. QQ Rate", f"${avg_qq_rate:.2f}")

    st.subheader("🔥 Sold Out Pressure")
    col5, col6 = st.columns(2)
    with col5:
        st.metric("👑 King Sold Out Days", num_king_sold_out)
    with col6:
        st.metric("🛏️ QQ Sold Out Days", num_qq_sold_out)

    st.subheader("🛠️ Maintenance Impact")
    col1, col2 = st.columns(2)
    with col1:
        st.metric(label=f"🔧 Total OOO Rooms\n(out of {total_days_in_db} days)", value=f"{int(total_ooo_rooms)}")
    with col2:
        st.metric(label="📉 % Days with OOO Rooms @ 9 PM", value=f"{ooo_day_percent:.1f}%")

    st.markdown("---")

    # --- Occupancy Rate Chart ---
    filtered_data['Occupancy Rate'] = filtered_data['Rooms Sold'] * 100.0 / MAX_HOTEL_CAPACITY
    plot_metric_chart(
        filtered_data,
        metric_col='Occupancy Rate',
        hour=21,
        title=f"Daily Occupancy Rate Trend by {time_scale}",
        y_title="Occupancy Rate (%)",
        graph_type=graph_type,
        time_scale=time_scale
    )
    # --- Arrivals Rate Chart ---
    plot_metric_chart(
        filtered_data,
        metric_col='Arrivals',
        hour=15,
        title=f"Daily Arrivals at 3 PM Trend by {time_scale}",
        y_title="Arrivals at 3 PM",
        graph_type=graph_type,
        time_scale=time_scale
    )

    # --- Out Of Order Rate Chart ---
    plot_metric_chart(
        filtered_data,
        metric_col='OOO Rooms',
        hour=21,
        title=f"OOO Rooms Trend at 9 PM Trend by {time_scale}",
        y_title="Out-of-Order Rooms",
        graph_type=graph_type,
        time_scale=time_scale
    )

    st.markdown("---") 

    st.subheader("Filtered by Date Data Preview")
    st.dataframe(all_data.tail(6)) 

else:
    st.warning("No data loaded. Please check your database file and sync script.")