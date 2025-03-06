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
tab1, tab2, tab3, tab4, tab5 = st.tabs(["Operational KPIs", "Temporal Analysis", "Environmental Impact", "Train & Station Metrics", "Platform Metrics"])


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
# Add to your existing "Train & Station Metrics" tab (tab4)
with tab5:
    st.header("🚉 Station-Specific Platform Analysis")
    
    # 1. Station Selection
    selected_station = st.selectbox(
        "Select Station",
        options=session.sql("SELECT DISTINCT CURRENT_STATION FROM FACT_CITY_MOVEMENT").to_pandas()['CURRENT_STATION'],
        key="platform_station_select"
    )
    
    # 2. Get Platforms for Selected Station
    @st.cache_data
    def get_station_platforms(station):
        return session.sql(f"""
            SELECT DISTINCT PLATFORM 
            FROM FACT_CITY_MOVEMENT 
            WHERE CURRENT_STATION = '{station}'
        """).to_pandas()['PLATFORM'].tolist()
    
    station_platforms = get_station_platforms(selected_station)
    
    if not station_platforms:
        st.warning("No platform data available for selected station")
        st.stop()
    
    # 3. Platform Performance Metrics
    st.subheader(f"📊 Platform Metrics for {selected_station}")
    
    platform_data = session.sql(f"""
        SELECT
            PLATFORM,
            AVG(DELAY_IN_MINUTES) AS avg_delay,
            COUNT(*) AS total_movements,
            AVG(TIMEDIFF(MINUTE, PLANNED_ARRIVAL_TIME, ARRIVAL_TIME)) AS time_variance,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY DELAY_IN_MINUTES) AS p95_delay,
            COUNT(DISTINCT TRAIN_ID) AS unique_trains,
            CORR(TEMPERATURE, DELAY_IN_MINUTES) AS temp_corr
        FROM FACT_CITY_MOVEMENT
        WHERE CURRENT_STATION = '{selected_station}'
          AND DATE BETWEEN '{date_range[0]}' AND '{date_range[1]}'
        GROUP BY PLATFORM
    """).to_pandas()

    # 4. Platform Comparison Visualization
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Total Platforms", len(station_platforms))
        st.dataframe(
            platform_data,
            column_config={
                "PLATFORM": "Platform",
                "avg_delay": st.column_config.NumberColumn(
                    "Avg Delay (min)",
                    format="%.1f"
                ),
                "unique_trains": "Unique Trains"
            },
            use_container_width=True
        )
    
    with col2:
        st.subheader("Platform Performance Comparison")
        if len(station_platforms) > 1:
            st.bar_chart(
                platform_data.set_index('PLATFORM')['AVG_DELAY'],
                color="#FF4B4B"
            )
        else:
            st.info("Single platform station - no comparison available")

    # 5. Space Utilization Analysis (Based on MDPI paper)
    st.subheader("🚦 Platform Space Utilization")
    space_metrics = session.sql(f"""
        SELECT
            PLATFORM,
            HOUR(ARRIVAL_TIME) AS hour,
            COUNT(*) AS movements,
            COUNT(DISTINCT TRAIN_ID) AS train_density,
            AVG(DELAY_IN_MINUTES) AS avg_delay
        FROM FACT_CITY_MOVEMENT
        WHERE CURRENT_STATION = '{selected_station}'
          AND DATE BETWEEN '{date_range[0]}' AND '{date_range[1]}'
        GROUP BY PLATFORM, hour
    """).to_pandas()

    selected_platform = st.selectbox(
        "Select Platform for Detailed Analysis",
        options=station_platforms,
        key="platform_detail_select"
    )
    
    platform_hourly = space_metrics[space_metrics['PLATFORM'] == selected_platform]
    
    tab_a, tab_b = st.tabs(["Congestion Patterns", "Train Density"])
    
    with tab_a:
        st.line_chart(
            platform_hourly.set_index('HOUR')[['AVG_DELAY', 'MOVEMENTS']],
            color=["#FF4B4B", "#0068C9"]
        )
    
    with tab_b:
        st.area_chart(
            platform_hourly.set_index('HOUR')['TRAIN_DENSITY'],
            color="#00CC96"
        )

    # 6. Capacity Analysis (Based on Collective Dynamics paper)
    st.subheader("🚧 Capacity Recommendations")
    capacity_data = session.sql(f"""
        WITH train_sequence AS (
            SELECT 
                PLATFORM,
                ARRIVAL_TIME,
                DELAY_IN_MINUTES,
                TIMEDIFF(
                    MINUTE, 
                    ARRIVAL_TIME,
                    LEAD(ARRIVAL_TIME) OVER (
                        PARTITION BY PLATFORM 
                        ORDER BY TIMESTAMP_NTZ_FROM_PARTS(DATE, ARRIVAL_TIME)
                    )
                ) AS time_gap
            FROM FACT_CITY_MOVEMENT
            WHERE CURRENT_STATION = '{selected_station}'
        )
        SELECT 
            PLATFORM,
            PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY DELAY_IN_MINUTES) AS peak_delay,
            AVG(time_gap) AS time_between_trains
        FROM train_sequence
        GROUP BY PLATFORM

    """).to_pandas()

    cols = st.columns(3)
    for idx, (_, row) in enumerate(capacity_data.iterrows()):
        with cols[idx%3]:
            st.metric(
                label=f"Platform {row['PLATFORM']}",
                value=f"{row['PEAK_DELAY']:.1f} min peak delay",
                delta=f"{(row['TIME_BETWEEN_TRAINS'] or 0):.1f} min avg gap"
            )

    # 7. Practical Recommendations (Based on Tri-Rail guidelines)
    with st.expander("🔧 Platform Optimization Tips"):
        st.write("""
        **Based on performance analysis:**
        - Consider platform lengthening for frequent peak-hour congestion
        - Add shelter coverage during high-delay periods
        - Implement dynamic signage for crowded platforms
        - Review maintenance schedules for high-usage platforms
        """)


# Data Freshness
last_updated = session.sql("SELECT MAX(LAST_UPDATED_DT) FROM FACT_CITY_MOVEMENT").collect()[0][0]
st.sidebar.caption(f"🗓️ Last Updated: {last_updated}")
st.sidebar.button("🔄 Refresh Data", help="Force refresh cached queries")

