# snowflake_streamlit_app.py
import streamlit as st
from snowflake.snowpark import Session

session = Session.builder.getOrCreate()

# Page Configuration
st.set_page_config(page_title="Rail Analytics", layout="wide")
st.title("🚄 City Movement Analytics Dashboard")

# Date Filter
with st.sidebar.expander("📅 Date Range Filter"):
    date_range = st.date_input("Select Range", [])

# Main KPI Grid
@st.cache_data
def get_summary_kpis():
    return session.sql(f"""
        SELECT
            COUNT(*) AS total_movements,
            AVG(DELAY_IN_MINUTES) AS avg_delay,
            SUM(CASE WHEN DELAY_IN_MINUTES <= 2 THEN 1 END) * 100.0 / COUNT(*) AS on_time_pct,
            CORR(TEMPERATURE, DELAY_IN_MINUTES) AS temp_corr,
            AVG(TIMEDIFF(MINUTE, PLANNED_ARRIVAL_TIME, ARRIVAL_TIME)) AS avg_time_variance
        FROM FACT_CITY_MOVEMENT
        WHERE DATE BETWEEN '{date_range[0]}' AND '{date_range[1]}'
    """).to_pandas()

kpis = get_summary_kpis()
cols = st.columns(5)
cols[0].metric("Total Movements", f"{kpis['TOTAL_MOVEMENTS'][0]:,}")
cols[1].metric("Avg Delay", f"{kpis['AVG_DELAY'][0]:.1f} min")
cols[2].metric("On-Time %", f"{kpis['ON_TIME_PCT'][0]:.1f}%")
cols[3].metric("Temp Impact", f"{kpis['TEMP_CORR'][0]:.2f}")
cols[4].metric("Time Variance", f"{kpis['AVG_TIME_VARIANCE'][0]:.1f} min")

# Main Visualization Tabs
tab1, tab2, tab3, tab4 = st.tabs(["Operational KPIs", "Temporal Analysis", "Environmental Impact", "Train & Station Metrics"])


with tab1:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🚉 Station Compliance Rates")
        compliance_data = session.sql(f"""
            SELECT CURRENT_STATION,
                   SUM(CASE WHEN VARIATION_STATUS = 'ON TIME' THEN 1 END) * 100.0 / COUNT(*) AS compliance_rate
            FROM FACT_CITY_MOVEMENT
            WHERE DATE BETWEEN '{date_range[0]}' AND '{date_range[1]}'
            GROUP BY CURRENT_STATION
        """).to_pandas()
        st.bar_chart(compliance_data.set_index('CURRENT_STATION'))

    with col2:
        st.subheader("🚂 Train Reliability Index")
        train_data = session.sql(f"""
            SELECT TRAIN_ID,
                   AVG(DELAY_IN_MINUTES) AS avg_delay,
                   COUNT(*) AS total_trips
            FROM FACT_CITY_MOVEMENT
            WHERE DATE BETWEEN '{date_range[0]}' AND '{date_range[1]}'
            GROUP BY TRAIN_ID
            ORDER BY avg_delay DESC
            LIMIT 10
        """).to_pandas()
        
        # Option 1: Use a bar chart instead
        st.bar_chart(train_data.set_index('TRAIN_ID')['AVG_DELAY'])

with tab2:
    st.subheader("⏱️ 7-Day Delay Trend")
    trend_data = session.sql(f"""
        SELECT DATE,
               AVG(DELAY_IN_MINUTES) OVER (
                   ORDER BY DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
               ) AS rolling_avg
        FROM FACT_CITY_MOVEMENT
        WHERE DATE BETWEEN '{date_range[0]}' AND '{date_range[1]}'
    """).to_pandas()
    st.line_chart(trend_data.set_index('DATE'))

    st.subheader("📅 Weekly Performance")
    weekly_data = session.sql(f"""
        SELECT DAYNAME(DATE) AS weekday,
               AVG(DELAY_IN_MINUTES) AS avg_delay,
               PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY DELAY_IN_MINUTES) AS p90_delay
        FROM FACT_CITY_MOVEMENT
        WHERE DATE BETWEEN '{date_range[0]}' AND '{date_range[1]}'
        GROUP BY weekday
    """).to_pandas()
    st.area_chart(weekly_data.set_index('WEEKDAY'))

with tab3:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🌡️ Temperature Impact")
        temp_data = session.sql(f"""
            SELECT TEMPERATURE,
                   AVG(DELAY_IN_MINUTES) AS avg_delay
            FROM FACT_CITY_MOVEMENT
            WHERE DATE BETWEEN '{date_range[0]}' AND '{date_range[1]}'
            GROUP BY TEMPERATURE
        """).to_pandas()
        st.scatter_chart(temp_data, x='TEMPERATURE', y='AVG_DELAY')

    with col2:
        st.subheader("⚠️ Extreme Weather Alerts")
        alerts_data = session.sql(f"""
            SELECT CITY,
                   SUM(CASE WHEN TEMPERATURE > 85 OR TEMPERATURE < 32 THEN 1 END) AS extreme_days,
                   AVG(DELAY_IN_MINUTES) AS avg_delay
            FROM FACT_CITY_MOVEMENT
            WHERE DATE BETWEEN '{date_range[0]}' AND '{date_range[1]}'
            GROUP BY CITY
        """).to_pandas()
        st.dataframe(alerts_data)

# Advanced Features
with st.expander("🔍 Drill-Down Analysis"):
    station = st.selectbox("Select Station", session.sql("SELECT DISTINCT CURRENT_STATION FROM FACT_CITY_MOVEMENT").to_pandas()['CURRENT_STATION'])
    station_data = session.sql(f"""
        SELECT DATE,
               AVG(DELAY_IN_MINUTES) AS delay,
               COUNT(*) AS movements
        FROM FACT_CITY_MOVEMENT
        WHERE CURRENT_STATION = '{station}'
        GROUP BY DATE
    """).to_pandas()
    st.line_chart(station_data.set_index('DATE'))

# Advanced Features
with st.expander("🔍 Drill-Down Analysis"):
    station = st.selectbox("Select Station", session.sql("SELECT DISTINCT CITY FROM FACT_CITY_MOVEMENT").to_pandas()['CITY'])
    station_data = session.sql(f"""
        SELECT DATE,
               AVG(DELAY_IN_MINUTES) AS delay,
               COUNT(*) AS movements
        FROM FACT_CITY_MOVEMENT
        WHERE CITY = '{station}'
        GROUP BY DATE
    """).to_pandas()
    st.line_chart(station_data.set_index('DATE'))

with tab4:
    st.header("🚉 Real-Time Station Metrics")
    
    # Station Selection
    selected_station = st.selectbox(
        "Select a Station",
        options=session.sql("SELECT DISTINCT CURRENT_STATION FROM FACT_CITY_MOVEMENT").to_pandas()['CURRENT_STATION']
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader(f"🚆 Next Hour Departures from {selected_station}")
        departures = session.sql(f"""
            SELECT 
                TRAIN_ID,
                PLANNED_ARRIVAL_TIME AS scheduled_departure,
                ARRIVAL_TIME AS estimated_departure,
                NEXT_STATION,
                DELAY_IN_MINUTES
            FROM FACT_CITY_MOVEMENT
            WHERE CURRENT_STATION = '{selected_station}'
              AND TIMESTAMP_NTZ_FROM_PARTS(CURRENT_DATE(), ARRIVAL_TIME)  -- Combine current date with time
                  BETWEEN CURRENT_TIMESTAMP() 
                  AND DATEADD(hour, 1, CURRENT_TIMESTAMP())
            ORDER BY ARRIVAL_TIME
        """).to_pandas()
        
        if not departures.empty:
            st.dataframe(
                departures.style.format({
                    'DELAY_IN_MINUTES': '{:.0f} min',
                    'scheduled_departure': lambda x: x.strftime('%H:%M'),
                    'estimated_departure': lambda x: x.strftime('%H:%M')
                }),
                use_container_width=True
            )
        else:
            st.info("No upcoming departures in the next hour")

    with col2:
        st.subheader(f"🚉 Next Hour Arrivals to {selected_station}")
        arrivals = session.sql(f"""
            SELECT 
                TRAIN_ID,
                PLANNED_ARRIVAL_TIME,
                ARRIVAL_TIME,
                CURRENT_STATION AS last_station,
                DELAY_IN_MINUTES
            FROM FACT_CITY_MOVEMENT
            WHERE NEXT_STATION = '{selected_station}'
            AND TIMESTAMP_NTZ_FROM_PARTS(CURRENT_DATE(), PLANNED_ARRIVAL_TIME)
              BETWEEN CURRENT_TIMESTAMP() 
              AND DATEADD(hour, 1, CURRENT_TIMESTAMP())
            ORDER BY PLANNED_ARRIVAL_TIME
        """).to_pandas()
        
        if not arrivals.empty:
            st.dataframe(
                arrivals.style.format({
                    'DELAY_IN_MINUTES': '{:.0f} min',
                    'PLANNED_ARRIVAL_TIME': lambda x: x.strftime('%H:%M'),
                    'ARRIVAL_TIME': lambda x: x.strftime('%H:%M')
                }),
                use_container_width=True
            )
        else:
            st.info("No scheduled arrivals in the next hour")

    st.header("🚂 Individual Train Status")
    
    # Train Selection
    @st.cache_data
    def get_train_options():
        train_df = session.sql("SELECT DISTINCT TRAIN_ID FROM FACT_CITY_MOVEMENT").to_pandas()
        return train_df['TRAIN_ID'].tolist()
    
    # Create dropdown with train IDs
    selected_train = st.selectbox(
        "Select Train ID",
        options=get_train_options(),
        index=0,  # Default to first option
        help="Select a train to view its current status"
    )
    
    train_status = session.sql(f"""
        SELECT 
            CURRENT_STATION,
            NEXT_STATION,
            ARRIVAL_TIME AS last_arrival,
            PLANNED_ARRIVAL_TIME AS scheduled_arrival,
            DELAY_IN_MINUTES,
            VARIATION_STATUS,
            LAST_UPDATED_DT
        FROM FACT_CITY_MOVEMENT
        WHERE TRAIN_ID = '{selected_train}'
        ORDER BY LAST_UPDATED_DT DESC
        LIMIT 1
    """).to_pandas()

    if not train_status.empty:
        status = train_status.iloc[0]
        delay_status = "🟢 On Time" if status['DELAY_IN_MINUTES'] <= 2 else \
                      "🟡 Minor Delay" if status['DELAY_IN_MINUTES'] <= 15 else \
                      "🔴 Significant Delay"
        
        cols = st.columns(4)
        cols[0].metric("Current Location", status['CURRENT_STATION'])
        cols[1].metric("Next Station", status['NEXT_STATION'])
        cols[2].metric("Schedule Status", delay_status)
        cols[3].metric("Last Update", status['LAST_UPDATED_DT'].strftime('%H:%M'))
        
        st.progress(
            min(status['DELAY_IN_MINUTES']/60, 1), 
            text=f"Delay: {status['DELAY_IN_MINUTES']} minutes"
        )
    else:
        st.warning("No recent data available for selected train")

# Data Freshness
last_updated = session.sql("SELECT MAX(LAST_UPDATED_DT) FROM FACT_CITY_MOVEMENT").collect()[0][0]
st.sidebar.caption(f"🗓️ Last Updated: {last_updated}")
st.sidebar.button("🔄 Refresh Data", help="Force refresh cached queries")

