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


def update_hourly_data():
    query = """
    INSERT INTO hourly_data (user_id, activity_hour, steps, calories, sleep_minutes, intensity_level, updated_at)
    SELECT 
        m.user_id,
        DATE_TRUNC('hour', m.activity_minute) AS activity_hour,
        SUM(m.steps) AS steps,
        SUM(m.calories) AS calories,
        SUM(m.sleep) AS sleep_minutes,
        ROUND(AVG(m.intensity), 2) AS intensity_level,
        NOW() AS updated_at
    FROM minute_data m
    LEFT JOIN hourly_data h 
        ON m.user_id = h.user_id 
        AND DATE_TRUNC('hour', m.activity_minute) = h.activity_hour
    WHERE h.activity_hour IS NULL  
    GROUP BY m.user_id, activity_hour
    ON CONFLICT (user_id, activity_hour) DO UPDATE 
    SET 
        steps = EXCLUDED.steps,
        calories = EXCLUDED.calories,
        sleep_minutes = EXCLUDED.sleep_minutes,
        intensity_level = EXCLUDED.intensity_level,
        updated_at = EXCLUDED.updated_at;
    """

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
        print("Hourly data updated successfully.")
    except Exception as e:
        print(f"Error updating hourly data: {e}")
    finally:
        cursor.close()
        conn.close()


def lambda_handler(event, context):
    update_hourly_data()
    return {"statusCode": 200, "body": "Hourly data updated successfully"}
