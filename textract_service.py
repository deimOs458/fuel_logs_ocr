import boto3
import os
from dotenv import load_dotenv
from utils import get_text, safe_int
import streamlit as st

textract = boto3.client(
    "textract",
    aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    region_name=st.secrets["AWS_DEFAULT_REGION"]
)

def extract_table_from_bytes(img_bytes, file_name):

    response = textract.analyze_document(
        Document={'Bytes': img_bytes},
        FeatureTypes=["TABLES"]
    )

    blocks = response['Blocks']
    block_map = {block['Id']: block for block in blocks}

    tables = []

    for block in blocks:
        if block['BlockType'] == 'TABLE':

            rows = {}

            for rel in block.get('Relationships', []):
                if rel['Type'] == 'CHILD':
                    for cell_id in rel['Ids']:
                        cell = block_map[cell_id]

                        if cell['BlockType'] == 'CELL':
                            r = cell['RowIndex']
                            c = cell['ColumnIndex']

                            if r not in rows:
                                rows[r] = {}

                            rows[r][c] = get_text(cell, block_map)

            table_data = []
            for r in sorted(rows.keys()):
                row = rows[r]
                ordered = [row.get(i, "") for i in sorted(row.keys())]
                table_data.append(ordered)

            tables.append(table_data)

    final_records = []

    if tables:
        table = tables[0]
        current_camp = None

        for row in table[1:]:
            if not any(row):
                continue

            row = row + [""] * (7 - len(row))

            if row[0]:
                current_camp = row[0]

            final_records.append({
                "Camp": current_camp,
                "Date": row[1],
                "Vehicle Reg": row[2],
                "Asset Type": row[3],
                "Company": row[4],
                "Litres Used": safe_int(row[5]),
                "Department": row[6]
            })

    return {
        "file_name": file_name,
        "total_records": len(final_records),
        "records": final_records
    }
