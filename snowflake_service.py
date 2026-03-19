import snowflake.connector
import os
import json
from dotenv import load_dotenv
load_dotenv()
def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

def insert_json(data):
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        INSERT INTO RAW_FUEL_LOGS (FILE_NAME, RAW_DATA)
        SELECT %s, PARSE_JSON(%s)
    """

    try:
        cursor.execute(query, (data["file_name"], json.dumps(data)))
        conn.commit()
        return True

    except Exception as e:
        raise e

    finally:
        cursor.close()
        conn.close()