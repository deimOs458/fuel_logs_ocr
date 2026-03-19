import streamlit as st
from dotenv import load_dotenv
from textract_service import extract_table_from_bytes
from snowflake_service import insert_json

load_dotenv()

# ---------- PAGE CONFIG ----------
st.set_page_config(
    page_title="Fortescue Fuel Log OCR",
    page_icon="⛽",
    layout="centered"
)

# ---------- HEADER ----------
st.markdown(
    """
    <h1 style='text-align: center;'>⛽ Fortescue Fuel Log OCR</h1>
    <p style='text-align: center; color: gray;'>
        Upload fuel log images and automatically extract & store data in Snowflake
    </p>
    """,
    unsafe_allow_html=True
)

st.divider()

# ---------- FILE UPLOAD ----------
uploaded_file = st.file_uploader(
    "📤 Upload Fuel Log Image",
    type=["jpg", "png", "jpeg"]
)

# ---------- PREVIEW ----------
if uploaded_file is not None:

    st.subheader("🖼️ Preview")
    st.image(uploaded_file, use_container_width=True)

    st.divider()

    # ---------- PROCESS BUTTON ----------
    if st.button("🚀 Process Image", use_container_width=True):

        with st.spinner("🔄 Processing with Textract..."):

            try:
                img_bytes = uploaded_file.read()

                result = extract_table_from_bytes(
                    img_bytes,
                    uploaded_file.name
                )

                # ---------- OUTPUT ----------
                st.subheader("📦 Extracted JSON")
                st.json(result)

                # ---------- SNOWFLAKE INSERT ----------
                insert_json(result)

                st.success("✅ Data successfully inserted into Snowflake")

            except Exception as e:
                st.error(f"❌ Error: {str(e)}")

# ---------- FOOTER ----------
st.markdown(
    """
    <hr>
    <p style='text-align: center; font-size: 12px; color: gray;'>
        Built for Fortescue | OCR Automation Pipeline
    </p>
    """,
    unsafe_allow_html=True
)