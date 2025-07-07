import streamlit as st
import pandas as pd
from utils.db import get_engine

# ========================================
# PAGE CONFIGURATION
# ========================================

st.set_page_config(
    page_title="Advanced Data Transformation",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========================================
# CUSTOM STYLES
# ========================================

custom_css = """
<style>
.stApp { background-color: #f5f5f5; }
.stButton>button {
    border: 2px solid #FF4B4B;
    border-radius: 20px;
    color: white;
    background-color: #FF4B4B;
    transition: all 0.3s;
}
.stButton>button:hover {
    background-color: white;
    color: #FF4B4B;
}
h1 { color: #4CAF50; text-align: center; }
</style>
"""
st.markdown(custom_css, unsafe_allow_html=True)

# ========================================
# DATA TRANSFORMATION
# ========================================

def apply_transformations(df: pd.DataFrame, selected_cols: list) -> pd.DataFrame | None:
    """Applies transformations to the selected columns and saves to a new table."""
    try:
        transformed_df = df.copy()
        numeric_cols = [col for col in selected_cols if pd.api.types.is_numeric_dtype(df[col])]

        if not numeric_cols:
            st.warning("No numeric columns selected for normalization.")
            return None

        # Normalization
        transformed_df[numeric_cols] = (df[numeric_cols] - df[numeric_cols].mean()) / df[numeric_cols].std()

        # Save to a new table
        engine = get_engine()
        if engine:
            transformed_df.to_sql(
                'transformed_data',
                engine,
                if_exists='replace',
                index=False,
                chunksize=10000
            )
            return transformed_df
        return None
    except Exception as e:
        st.error(f"Error during transformation: {str(e)}")
        return None

# ========================================
# MAIN APPLICATION
# ========================================

st.title("🔄 Advanced Data Transformation")

if st.button("Load Data to Transform"):
    engine = get_engine()
    if engine:
        try:
            df = pd.read_sql("SELECT * FROM raw_data LIMIT 10000", engine)
            if not df.empty:
                st.session_state.df_to_transform = df
                st.success("Data loaded successfully.")
            else:
                st.warning("No data available to transform. Please process a large file first.")
        except Exception as e:
            st.error(f"Could not load data: {e}")

if 'df_to_transform' in st.session_state:
    df = st.session_state.df_to_transform
    st.dataframe(df.head())

    st.subheader("Transformation Options")
    available_cols = df.columns.tolist()
    selected_cols = st.multiselect("Select columns to transform", available_cols)

    if selected_cols and st.button("Apply Selected Transformations"):
        with st.spinner("Transforming data..."):
            transformed_df = apply_transformations(df, selected_cols)
            if transformed_df is not None:
                st.success("✅ Transformations complete and saved to the database!")
                st.dataframe(transformed_df.head())