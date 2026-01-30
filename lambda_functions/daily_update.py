import psycopg2
import os

DB_CONFIG = {
    'host': 'database-1.cb0gsmcyc0ns.us-west-1.rds.amazonaws.com',
    'port': 5432,
    'dbname': 'fitbit_data',
    'user': 'postgres',
    'password': 'Pass1234!'
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def update_daily_data():
    query = """
    INSERT INTO daily_data (
        user_id, 
        activity_date, 
        total_steps, 
        total_distance, 
        total_calories, 
        total_sleep_minutes, 
        very_active_minutes, 
        fairly_active_minutes, 
        lightly_active_minutes, 
        sedentary_minutes, 
        updated_at
    )
    SELECT 
        h.user_id,
        DATE(h.activity_hour) AS activity_date,
        SUM(h.steps) AS total_steps,
        ROUND(SUM(h.steps) * 0.000762, 2) AS total_distance, 
        SUM(h.calories) AS total_calories,
        SUM(h.sleep_minutes) AS total_sleep_minutes,

        SUM(CASE WHEN h.intensity_level >= 75 THEN 1 ELSE 0 END) AS very_active_minutes,
        SUM(CASE WHEN h.intensity_level BETWEEN 40 AND 74 THEN 1 ELSE 0 END) AS fairly_active_minutes,
        SUM(CASE WHEN h.intensity_level BETWEEN 20 AND 39 THEN 1 ELSE 0 END) AS lightly_active_minutes,
        SUM(CASE WHEN h.intensity_level < 20 THEN 1 ELSE 0 END) AS sedentary_minutes,

        NOW() AS updated_at
    FROM hourly_data h
    LEFT JOIN daily_data d 
        ON h.user_id = d.user_id 
        AND DATE(h.activity_hour) = d.activity_date
    WHERE d.activity_date IS NULL  
    GROUP BY h.user_id, activity_date
    ON CONFLICT (user_id, activity_date) DO UPDATE 
    SET 
        total_steps = EXCLUDED.total_steps,
        total_distance = EXCLUDED.total_distance,
        total_calories = EXCLUDED.total_calories,
        total_sleep_minutes = EXCLUDED.total_sleep_minutes,
        very_active_minutes = EXCLUDED.very_active_minutes,
        fairly_active_minutes = EXCLUDED.fairly_active_minutes,
        lightly_active_minutes = EXCLUDED.lightly_active_minutes,
        sedentary_minutes = EXCLUDED.sedentary_minutes,
        updated_at = EXCLUDED.updated_at;
    """

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
        print("Daily data updated successfully.")
    except Exception as e:
        print(f"Error updating daily data: {e}")
    finally:
        cursor.close()
        conn.close()


def lambda_handler(event, context):
    update_daily_data()
    return {"statusCode": 200, "body": "Daily data updated successfully"}
