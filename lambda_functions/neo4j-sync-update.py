
import json
import psycopg2
import datetime
import json
from neo4j import GraphDatabase

# RDS credentials
RDS_HOST = "database-1.cb0gsmcyc0ns.us-west-1.rds.amazonaws.com"
RDS_DB = "fitbit_data"
RDS_USER = "postgres"
RDS_PASSWORD = "Pass1234!"

# # Neo4j credentials
NEO4J_URI = "neo4j+s://9d24ccb0.databases.neo4j.io"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "ll0grkN_ME_vUSDlN2JMRd12xZ8m8Qmj25M0e5LZFuA"


def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host=RDS_HOST,
        database=RDS_DB,
        user=RDS_USER,
        password=RDS_PASSWORD,
        port=5432
    )


def get_neo4j_session():
    """Establishes a connection to the Neo4j database."""
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    return driver.session()


def fetch_daily_metrics(user_id):
    """Fetches daily metrics and computes intensity & resting heart rate."""
    query = """
    WITH latest_metrics AS (
        SELECT
            d.user_id,
            d.activity_date,
            d.total_steps,
            d.total_calories,
            d.total_sleep_minutes,
            (
                SELECT ROUND(AVG(h.intensity_level), 2)
                FROM hourly_data h
                WHERE d.user_id = h.user_id
                  AND DATE(h.activity_hour) = d.activity_date
            ) AS avg_intensity,
            (
                SELECT mode() WITHIN GROUP (ORDER BY m.heart_rate)
                FROM minute_data m
                WHERE d.user_id = m.user_id
                  AND DATE(m.activity_minute) = d.activity_date
            ) AS resting_hr
        FROM daily_data d
        WHERE d.user_id = %s
        ORDER BY d.activity_date DESC
        LIMIT 1
    )
    SELECT * FROM latest_metrics;
    """

    with get_db_connection() as conn, conn.cursor() as cursor:
        cursor.execute(query, (user_id,))
        result = cursor.fetchone()

    if not result:
        return None

    return {
        "user_id": result[0],
        "activity_date": str(result[1]),
        "total_steps": result[2],
        "total_calories": result[3],
        "total_sleep_minutes": result[4],
        "avg_intensity": result[5],
        "resting_hr": result[6]
    }


def fetch_met_values(user_id):
    """Fetches MET averages split by time zones for the last 7 days."""
    query = """
    WITH latest_user_date AS (
        SELECT DATE(MAX(activity_minute)) AS latest_date
        FROM minute_data
        WHERE user_id = %s
    ),
    met_by_timezone AS (
        SELECT
            m.user_id,
            ROUND(AVG(CASE WHEN EXTRACT(HOUR FROM m.activity_minute) BETWEEN 0 AND 5 THEN m.mets ELSE NULL END), 2) AS early_morning_avg_met,
            ROUND(AVG(CASE WHEN EXTRACT(HOUR FROM m.activity_minute) BETWEEN 6 AND 11 THEN m.mets ELSE NULL END), 2) AS morning_avg_met,
            ROUND(AVG(CASE WHEN EXTRACT(HOUR FROM m.activity_minute) BETWEEN 12 AND 17 THEN m.mets ELSE NULL END), 2) AS afternoon_avg_met,
            ROUND(AVG(CASE WHEN EXTRACT(HOUR FROM m.activity_minute) BETWEEN 18 AND 23 THEN m.mets ELSE NULL END), 2) AS evening_avg_met
        FROM minute_data m
        JOIN latest_user_date l ON DATE(m.activity_minute) BETWEEN l.latest_date - INTERVAL '6 days' AND l.latest_date
        WHERE m.user_id = %s
        GROUP BY m.user_id
    )
    SELECT 
        tz.user_id,
        tz.early_morning_avg_met, em_md.likely_activity AS early_morning_likely_activity,
        tz.morning_avg_met, m_md.likely_activity AS morning_likely_activity,
        tz.afternoon_avg_met, a_md.likely_activity AS afternoon_likely_activity,
        tz.evening_avg_met, e_md.likely_activity AS evening_likely_activity
    FROM met_by_timezone tz
    LEFT JOIN met_data em_md ON tz.early_morning_avg_met BETWEEN em_md.met_min AND em_md.met_max
    LEFT JOIN met_data m_md ON tz.morning_avg_met BETWEEN m_md.met_min AND m_md.met_max
    LEFT JOIN met_data a_md ON tz.afternoon_avg_met BETWEEN a_md.met_min AND a_md.met_max
    LEFT JOIN met_data e_md ON tz.evening_avg_met BETWEEN e_md.met_min AND e_md.met_max;
    """

    with get_db_connection() as conn, conn.cursor() as cursor:
        cursor.execute(query, (user_id, user_id))
        result = cursor.fetchone()

    if not result:
        return None

    return {
        "user_id": result[0],
        "early_morning": {"avg_met": result[1], "activity": result[2]},
        "morning": {"avg_met": result[3], "activity": result[4]},
        "afternoon": {"avg_met": result[5], "activity": result[6]},
        "evening": {"avg_met": result[7], "activity": result[8]},
    }


def user_exists_in_neo4j(session, user_id):
    """Check if the user already exists in Neo4j."""
    result = session.run(
        """OPTIONAL MATCH (u:User {user_id: $user_id})
            RETURN 
                CASE 
                    WHEN u IS NOT NULL THEN "User Found"
                    ELSE "User Not Found"
            END AS result
        """,
        user_id=str(user_id)
    )
    if result.single()['result'] == "User Found":
        return True
    return False


def create_or_update_user_in_neo4j(session, user_id, age, smoker, drinker, bmi):
    """Create a new user node in Neo4j, handling nulls."""
    session.run(
        """
        MERGE (u:User {user_id: $user_id})
        SET
            u.age = coalesce($age, u.age),
            u.bmi = coalesce($bmi, u.bmi),
            u.smoker = coalesce($smoker, u.smoker),
            u.drinker = coalesce($drinker, u.drinker),
            u.updated_at = date($updated_at)
        """,
        user_id=int(user_id),
        age=int(age) if age is not None else None,  # Handle null age
        # Handle null smoker
        smoker=bool(smoker) if smoker is not None else None,
        # Handle null drinker
        drinker=bool(drinker) if drinker is not None else None,
        bmi=float(bmi) if bmi is not None else None,  # Handle null bmi
        updated_at=datetime.datetime.now().date().isoformat()
    )


def update_user_metrics(session, user_id, metrics):
    """Update user metrics in Neo4j, handling nulls."""
    session.run(
        """
        MATCH (u:User {user_id: $user_id})
        MERGE (dm:DailyMetric {user_id: $user_id})
        SET dm.activity_date = coalesce(date($activity_date), dm.activity_date),
            dm.total_steps = coalesce($total_steps, dm.total_steps),
            dm.total_calories = coalesce($total_calories, dm.total_calories),
            dm.total_sleep_minutes = coalesce($total_sleep_minutes, dm.total_sleep_minutes),
            dm.average_intensity = coalesce($average_intensity, dm.average_intensity),
            dm.resting_hr = coalesce($resting_hr, dm.resting_hr)
        MERGE (u)-[:HAS_DAILY_METRIC]->(dm)
        RETURN dm
        """,
        user_id=int(user_id),
        # Directly use .get() to avoid KeyError
        activity_date=metrics.get('activity_date'),
        total_steps=int(metrics.get('total_steps', 0)) if metrics.get(
            'total_steps') is not None else None,
        total_calories=float(metrics.get('total_calories', 0.0)) if metrics.get(
            'total_calories') is not None else None,
        total_sleep_minutes=int(metrics.get('total_sleep_minutes', 0)) if metrics.get(
            'total_sleep_minutes') is not None else None,
        average_intensity=float(metrics.get('avg_intensity', 0.0)) if metrics.get(
            'avg_intensity') is not None else None,
        resting_hr=float(metrics.get('resting_hr', 0.0)) if metrics.get(
            'resting_hr') is not None else None
    )


def update_met_values(session, user_id, met_values):
    """Update MET nodes for the user in Neo4j, handling nulls."""
    session.run(
        """
        MATCH (u:User {user_id: $user_id})
        MERGE (m:MET {user_id: $user_id})
        SET m.early_morning_avg_met = coalesce($early_morning_avg_met, m.early_morning_avg_met),
            m.morning_avg_met = coalesce($morning_avg_met, m.morning_avg_met),
            m.afternoon_avg_met = coalesce($afternoon_avg_met, m.afternoon_avg_met),
            m.evening_avg_met = coalesce($evening_avg_met, m.evening_avg_met)
        MERGE (u)-[:HAS_MET]->(m)
        RETURN m
        """,
        user_id=int(user_id),
        early_morning_avg_met=float(met_values.get('early_morning', {}).get(
            'avg_met', 0.0)) if met_values.get('early_morning') else None,
        morning_avg_met=float(met_values.get('morning', {}).get(
            'avg_met', 0.0)) if met_values.get('morning') else None,
        afternoon_avg_met=float(met_values.get('afternoon', {}).get(
            'avg_met', 0.0)) if met_values.get('afternoon') else None,
        evening_avg_met=float(met_values.get('evening', {}).get(
            'avg_met', 0.0)) if met_values.get('evening') else None
    )


def lambda_handler(event, context):
    user_id = event.get("user_id")
    event_type = event.get("event_type")
    user_id = int(user_id)
    print("Invoked From TRIGGER!!!!")
    if not user_id:
        return {"error": "user_id is required"}

    with get_neo4j_session() as neo4j_session:

        # Check if user already exists in Neo4j
        if not user_exists_in_neo4j(neo4j_session, user_id):

            # Fetch the new user demographics and add to Neo4j
            with get_db_connection() as conn, conn.cursor() as cursor:
                cursor.execute(
                    "SELECT age, smoker, drinker, bmi FROM user_demographics WHERE user_id = %s",
                    (user_id,)
                )
                demo = cursor.fetchone()
                print(demo)
            if demo:
                create_or_update_user_in_neo4j(
                    neo4j_session, user_id, demo[0], demo[1], demo[2], demo[3])
            # else:
            #     create_or_update_user_in_neo4j(neo4j_session, user_id, None, None, None, None)
        # Fetch the latest daily metrics
        metrics = fetch_daily_metrics(user_id)
        if metrics:
            update_user_metrics(neo4j_session, user_id, metrics)

        # Fetch and update MET values
        met_values = fetch_met_values(user_id)
        if met_values:
            update_met_values(neo4j_session, user_id, met_values)

    return {"message": "User data processed successfully!"}
