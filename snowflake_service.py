import snowflake.connector
import os
import json
from dotenv import load_dotenv
secret_blob = os.environ.get("env_secret", "")
for line in secret_blob.splitlines():
    if "=" in line:
        key, value = line.split("=", 1)
        os.environ[key] = value

# ---------- SNOWFLAKE CONNECTION ----------
import snowflake.connector
import streamlit as st

def get_connection():
    return snowflake.connector.connect(
        user=st.secrets["SNOWFLAKE_USER"],
        password=st.secrets["SNOWFLAKE_PASSWORD"],
        account=st.secrets["SNOWFLAKE_ACCOUNT"],
        warehouse=st.secrets["SNOWFLAKE_WAREHOUSE"],
        database=st.secrets["SNOWFLAKE_DATABASE"],
        schema=st.secrets["SNOWFLAKE_SCHEMA"]
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
