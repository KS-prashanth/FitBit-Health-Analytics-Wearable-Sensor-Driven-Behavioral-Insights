import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from neo4j import GraphDatabase
from datetime import date, datetime, time, timedelta  # ✅ added

# Neo4j credentials
NEO4J_URI = "neo4j+ssc://9d24ccb0.databases.neo4j.io"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "omVxl-U8whzzk-jKPygO-TElJr-986SgKECIgcoVkec"

# Database connection
DB_CONFIG = {
    'host': 'database-1.cfkiysy2kojp.us-east-2.rds.amazonaws.com',
    'port': 5432,
    'dbname': 'testname',
    'user': 'postgres',
    'password': '5185148868K$p',
    'sslmode': 'require'   # ✅ IMPORTANT for AWS RDS
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_data(query, params=None):
    conn = get_connection()
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

# Streamlit UI
st.title("📊 Fitbit Health Dashboard")

# -----------------------------
# User Login (Dropdown from DB)
# -----------------------------
st.sidebar.header("User Login")

user_ids_df = fetch_data("SELECT DISTINCT user_id FROM daily_data ORDER BY user_id")
user_ids = user_ids_df["user_id"].dropna().astype(int).tolist()

if not user_ids:
    st.error("No user IDs found in daily_data.")
    st.stop()

user_id = st.sidebar.selectbox("Select User ID", user_ids)

# ----------------------------------------
# Date selection (ONLY Fitabase date range)
# ----------------------------------------
FITABASE_MIN_DATE = date(2016, 3, 12)
FITABASE_MAX_DATE = date(2016, 5, 12)

selected_date = st.sidebar.date_input(
    "Select Date",
    value=FITABASE_MAX_DATE,
    min_value=FITABASE_MIN_DATE,
    max_value=FITABASE_MAX_DATE
)
st.sidebar.caption(f"Data available: {FITABASE_MIN_DATE} → {FITABASE_MAX_DATE}")

# (Keep this if you want; not required anymore for the fallback)
latest_hour_query = """
    SELECT MAX(EXTRACT(HOUR FROM activity_hour)) 
    FROM hourly_data 
    WHERE user_id = %s AND activity_hour::DATE = %s
"""
latest_hour_result = fetch_data(latest_hour_query, (user_id, selected_date))
latest_hour = latest_hour_result.iloc[0, 0] if not latest_hour_result.empty else None
if latest_hour is None:
    latest_hour = 23
else:
    latest_hour = int(latest_hour)

# Fetch daily stats or aggregate hourly data if daily stats not available
daily_stats_query = """
    SELECT total_steps, total_calories, total_sleep_minutes, 
           (
            SELECT mode() WITHIN GROUP (ORDER BY heart_rate) 
            FROM minute_data 
            WHERE user_id = %s AND activity_minute::DATE = %s
            ) AS resting_hr
    FROM daily_data WHERE user_id = %s AND activity_date = %s
"""
daily_stats = fetch_data(
    daily_stats_query, (user_id, selected_date, user_id, selected_date))

# ✅ FIXED fallback (no "None:59:59" timestamps)
if daily_stats.empty:
    data_query = """
        SELECT SUM(steps) AS total_steps, SUM(calories) AS total_calories, 
               SUM(sleep_minutes) AS total_sleep_minutes, 
               (
                SELECT mode() WITHIN GROUP (ORDER BY heart_rate) 
                FROM minute_data 
                WHERE user_id = %s AND activity_minute::DATE = %s
                ) AS resting_hr
        FROM hourly_data WHERE user_id = %s AND activity_hour >= %s AND activity_hour < %s
    """

    start_ts = datetime.combine(selected_date, time.min)
    end_ts = start_ts + timedelta(days=1)

    params = (user_id, selected_date, user_id, start_ts, end_ts)
    daily_stats = fetch_data(data_query, params)

# Fetch intensity data
intensity_query = """
    SELECT ROUND(AVG(intensity_level), 2) AS avg_intensity
    FROM hourly_data
    WHERE user_id = %s AND activity_hour::DATE = %s
"""
intensity_data = fetch_data(intensity_query, (user_id, selected_date))
avg_intensity = intensity_data.iloc[0, 0] if not intensity_data.empty else None

# Fetch anomaly thresholds
anomaly_query = """
    SELECT metric_name, min_value, max_value FROM metric_explaination
    WHERE metric_name IN ('Sleep (Hours)', 'Heart Rate (Resting bpm)', 'Intensity (HRV in ms)', 'Calories (kcal/day)')
"""
anomaly_thresholds = fetch_data(anomaly_query)

# Convert thresholds into a dictionary
thresholds = {row['metric_name']: (row['min_value'], row['max_value'])
              for _, row in anomaly_thresholds.iterrows()}

# Function to detect anomalies
def detect_anomaly(metric, value):
    if metric in thresholds and value is not None and not pd.isna(value):
        min_val, max_val = thresholds[metric]
        return value < min_val or value > max_val
    return False

# Display Metrics
if not daily_stats.empty:
    st.write("### Health Metrics for Selected Day")
    st.metric("Total Steps", f"{int(daily_stats['total_steps'].sum())} steps")
    st.metric("Total Calories", f"{int(daily_stats['total_calories'].sum())} kcal")
    st.metric("Total Sleep", f"{int(daily_stats['total_sleep_minutes'].sum())} min")
    st.metric("Resting Heart Rate", f"{int(daily_stats['resting_hr'].sum())} bpm")
    st.metric("Average Intensity", f"{avg_intensity} ms")

    anomalies = []
    if detect_anomaly("Sleep (Hours)", daily_stats["total_sleep_minutes"].sum() / 60):
        anomalies.append("⚠️ Unusual sleep duration detected! (in hours)")
    if detect_anomaly("Heart Rate (Resting bpm)", daily_stats["resting_hr"].sum()):
        anomalies.append("⚠️ Abnormal resting heart rate detected! (bpm)")
    if detect_anomaly("Intensity (HRV in ms)", avg_intensity):
        anomalies.append("⚠️ Unusual intensity levels detected! (ms)")
    if detect_anomaly("Calories (kcal/day)", daily_stats["total_calories"].sum()):
        anomalies.append("⚠️ Caloric expenditure outside normal range! (kcal)")

    if anomalies:
        st.warning("\n".join(anomalies))

# Historical Data Query (with Intensity)
historical_query = """
    SELECT activity_date, total_steps, total_calories, total_sleep_minutes, 
           (
            SELECT mode() WITHIN GROUP (ORDER BY heart_rate) 
            FROM minute_data 
            WHERE user_id = %s AND activity_minute::DATE = d.activity_date
            ) AS resting_hr,
           (
            SELECT ROUND(AVG(intensity_level), 2)
            FROM hourly_data h
            WHERE h.user_id = d.user_id
            AND DATE(h.activity_hour) = d.activity_date
            ) AS avg_intensity
    FROM daily_data d WHERE user_id = %s ORDER BY activity_date
"""
historical_data = fetch_data(historical_query, (user_id, user_id))

# Historical Data Visualization with Anomalies
if not historical_data.empty:
    metric_choice = st.selectbox("Select a metric to visualize", [
        "Total Steps", "Total Calories", "Total Sleep", "Resting HR", "Average Intensity"
    ])

    metric_mapping = {
        "Total Steps": "total_steps",
        "Total Calories": "total_calories",
        "Total Sleep": "total_sleep_minutes",
        "Resting HR": "resting_hr",
        "Average Intensity": "avg_intensity"
    }

    threshold_mapping = {
        "Total Calories": "Calories (kcal/day)",
        "Total Sleep": "Sleep (Hours)",
        "Resting HR": "Heart Rate (Resting bpm)",
        "Average Intensity": "Intensity (HRV in ms)"
    }

    if metric_choice in threshold_mapping:
        anomaly_metric = threshold_mapping[metric_choice]
        historical_data["is_anomaly"] = historical_data[metric_mapping[metric_choice]].apply(
            lambda x: detect_anomaly(anomaly_metric, x if metric_choice != "Total Sleep" else x / 60)
        )
    else:
        historical_data["is_anomaly"] = False

    fig = px.line(historical_data, x="activity_date", y=metric_mapping[metric_choice],
                  title=f"Historical {metric_choice}")

    anomalies_df = historical_data[historical_data["is_anomaly"]]
    fig.add_scatter(
        x=anomalies_df["activity_date"],
        y=anomalies_df[metric_mapping[metric_choice]],
        mode='markers',
        marker=dict(color='red', size=8),
        name="Anomaly"
    )

    st.plotly_chart(fig)

# -------------------------
# Neo4j MET & Recommendations
# -------------------------
def get_neo4j_session():
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        return driver.session()
    except Exception as e:
        st.error(f"Neo4j Connection Error: {str(e)}")
        return None

def fetch_met_data(user_id):
    query = """
    MATCH (u:User {user_id: $user_id})-[:HAS_MET]->(m:MET)
    RETURN 
        u.user_id AS user_id,
        m.early_morning_avg_met AS early_morning_avg_met, m.early_morning_likely_activity AS early_morning_likely_activity,
        m.morning_avg_met AS morning_avg_met, m.morning_likely_activity AS morning_likely_activity,
        m.afternoon_avg_met AS afternoon_avg_met, m.afternoon_likely_activity AS afternoon_likely_activity,
        m.evening_avg_met AS evening_avg_met, m.evening_likely_activity AS evening_likely_activity
    """
    session = get_neo4j_session()
    if session is None:
        return None
    with session as s:
        result = s.run(query, user_id=int(user_id))
        data = result.single()

    if data:
        return dict(data)
    return None

met_data = fetch_met_data(user_id)

if met_data:
    st.write("### Last 7 Days MET Insights")
    st.metric("Early Morning MET", met_data["early_morning_avg_met"], met_data["early_morning_likely_activity"])
    st.metric("Morning MET", met_data["morning_avg_met"], met_data["morning_likely_activity"])
    st.metric("Afternoon MET", met_data["afternoon_avg_met"], met_data["afternoon_likely_activity"])
    st.metric("Evening MET", met_data["evening_avg_met"], met_data["evening_likely_activity"])
else:
    st.warning("No MET data available for this user.")

def get_recommendations(user_id: int):
    query = """
    MATCH (u:User {user_id: $user_id})
        OPTIONAL MATCH (u)-[:BELONGS_TO]->(c)
        WHERE c:ClusterA OR c:ClusterB OR c:ClusterC OR c:ClusterD
        OPTIONAL MATCH (c)-[:CONTAINS]->(peer:User)
        OPTIONAL MATCH (peer)-[:HAS_DAILY_METRIC]->(dm:DailyMetric)
        WHERE dm.total_sleep_minutes IS NOT NULL OR 
                dm.total_calories IS NOT NULL OR
                dm.average_intensity IS NOT NULL
        WITH u, 
            AVG(dm.total_sleep_minutes) AS avg_cluster_sleep,
            AVG(dm.total_calories) AS avg_cluster_calories,
            AVG(dm.average_intensity) AS avg_cluster_intensity
        RETURN u.user_id AS user_id, 
            "Recommended Sleep (minutes):" AS sleep_label,
            CASE 
                WHEN avg_cluster_sleep IS NOT NULL AND avg_cluster_sleep < 420 
                THEN "Try increasing your sleep duration for better recovery."
                WHEN avg_cluster_sleep IS NOT NULL AND avg_cluster_sleep > 540 
                THEN "Consider reducing excess sleep to improve energy levels."
                ELSE "Your sleep pattern is optimal."
            END AS sleep_recommendation,
            "Recommended Calories:" AS calories_label,
            CASE 
                WHEN avg_cluster_calories IS NOT NULL AND avg_cluster_calories < 2000 
                THEN "Increase calorie intake to improve energy levels."
                WHEN avg_cluster_calories IS NOT NULL AND avg_cluster_calories >= 2000 
                    AND avg_cluster_calories <= 2500 
                THEN "Your calorie intake is optimal."
                ELSE "Reduce calorie intake to manage weight effectively."
            END AS calories_recommendation,
            "Recommended Intensity:" AS intensity_label,
            CASE 
                WHEN avg_cluster_intensity IS NOT NULL AND avg_cluster_intensity < 30 
                THEN "Increase your workout intensity for better cardiovascular health."
                WHEN avg_cluster_intensity IS NOT NULL AND avg_cluster_intensity >= 30 
                    AND avg_cluster_intensity <= 70 
                THEN "Your intensity level is well-balanced."
                ELSE "Consider reducing high-intensity workouts to improve recovery."
            END AS intensity_recommendation
    """
    session = get_neo4j_session()
    if session is None:
        return None
    with session as s:
        result = s.run(query, user_id=int(user_id))
        record = result.single()
        if record:
            return {
                "sleep_label": record["sleep_label"],
                "sleep_recommendation": record["sleep_recommendation"],
                "calories_label": record["calories_label"],
                "calories_recommendation": record["calories_recommendation"],
                "intensity_label": record["intensity_label"],
                "intensity_recommendation": record["intensity_recommendation"]
            }
        else:
            return None

# Streamlit UI
st.title("Health Recommendations Based on Similar Profiles")

recommendations = get_recommendations(user_id)
if recommendations:
    st.subheader("Your Recommendations")
    st.write(f"**{recommendations['sleep_label']}** {recommendations['sleep_recommendation']}")
    st.write(f"**{recommendations['calories_label']}** {recommendations['calories_recommendation']}")
    st.write(f"**{recommendations['intensity_label']}** {recommendations['intensity_recommendation']}")
else:
    st.warning("No recommendations found. Please check your User ID or try again later.")
