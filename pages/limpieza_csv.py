

"""
Data Cleaner Pro - CSV Cleaning Tool with PostgreSQL
===========================================================

A Streamlit application for cleaning and processing CSV files with storage in PostgreSQL.
It supports both in-memory cleaning for small files and chunk processing for large files.

Author: [Your Name]
Date: 2025
Version: 1.3
"""

import streamlit as st
import pandas as pd
import base64
from io import StringIO, BytesIO
import logging
import csv
from utils.db import get_engine
from config import CHUNK_SIZE

# ========================================
# INITIAL CONFIGURATION
# ========================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Data Cleaner Pro",
    page_icon="🧹",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========================================
# SESSION STATE INITIALIZATION
# ========================================

def initialize_session_state():
    """Initializes all necessary session_state variables."""
    for key in ['cleaned_data', 'cleaning_stats', 'raw_data_loaded']:
        if key not in st.session_state:
            st.session_state[key] = None

# ========================================
# CUSTOM STYLES
# ========================================

def apply_custom_styles():
    """Applies custom CSS styles to the application."""
    st.markdown("""
    <style>
    .stApp { background-color: #f5f5f5; }
    .stButton>button {
        border-radius: 20px; transition: all 0.3s; font-weight: bold;
    }
    .stButton>button:hover {
        transform: translateY(-2px); box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    h1 { color: #FF4B4B; text-align: center; margin-bottom: 2rem; }
    .stFileUploader>div>div>div>div {
        border: 2px dashed #4CAF50; border-radius: 10px; padding: 2rem;
    }
    #MainMenu, footer { visibility: hidden; }
    </style>
    """, unsafe_allow_html=True)

# ========================================
# DATABASE & DOWNLOAD MANAGERS
# ========================================

class DatabaseManager:
    """Handles database operations."""
    def __init__(self):
        self.engine = get_engine()

    def save_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace') -> bool:
        if not self.engine:
            st.error("Database connection is not available.")
            return False
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False, chunksize=10000, method='multi')
            return True
        except Exception as e:
            st.error(f"Error saving to database: {e}")
            return False

class DownloadManager:
    """Handles the generation of download files."""
    @staticmethod
    def create_download_link(df: pd.DataFrame, filename: str, label: str, file_format: str) -> str:
        if file_format == 'csv':
            data = df.to_csv(index=False, encoding='utf-8-sig').encode()
            mime = 'text/csv'
        elif file_format == 'excel':
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='CleanedData')
            data = output.getvalue()
            mime = 'application/vnd.ms-excel'
        
        b64 = base64.b64encode(data).decode()
        return f'<a href="data:{mime};base64,{b64}" download="{filename}">📥 {label}</a>'

# ========================================
# FILE PROCESSOR
# ========================================

class FileProcessor:
    """Handles processing and cleaning of CSV files."""
    def __init__(self, db_manager: DatabaseManager = None):
        self.db_manager = db_manager

    def process_large_file(self, file) -> bool:
        """Processes large files in chunks and saves them to the database."""
        try:
            file.seek(0)
            delimiter = self.detect_delimiter(file.read(4096).decode('utf-8', errors='ignore'))
            file.seek(0)
            chunk_iter = pd.read_csv(file, chunksize=CHUNK_SIZE, delimiter=delimiter, low_memory=False)
            is_first_chunk = True
            for chunk in chunk_iter:
                if_exists = 'replace' if is_first_chunk else 'append'
                if not self.db_manager.save_dataframe(chunk, 'raw_data', if_exists):
                    return False
                is_first_chunk = False
            return True
        except Exception as e:
            st.error(f"Error processing large file: {e}")
            return False

    def process_regular_file(self, file) -> pd.DataFrame | None:
        """Processes a regular-sized CSV file in memory."""
        try:
            file.seek(0)
            content = file.getvalue().decode('utf-8')
            delimiter = self.detect_delimiter(content)
            return pd.read_csv(StringIO(content), delimiter=delimiter)
        except Exception as e:
            st.error(f"Error processing file {file.name}: {e}")
            return None

    def clean_dataframe(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
        """Cleans a DataFrame and returns it with cleaning stats."""
        initial_shape = df.shape
        df_cleaned = df.dropna(how='all').dropna(axis=1, how='all').drop_duplicates().reset_index(drop=True)
        final_shape = df_cleaned.shape
        stats = {
            'filas_iniciales': initial_shape[0], 'filas_finales': final_shape[0],
            'columnas_iniciales': initial_shape[1], 'columnas_finales': final_shape[1],
            'filas_eliminadas': initial_shape[0] - final_shape[0],
            'columnas_eliminadas': initial_shape[1] - final_shape[1]
        }
        return df_cleaned, stats

    def detect_delimiter(self, sample_content: str) -> str:
        """Detects the best delimiter for a CSV file."""
        try:
            return csv.Sniffer().sniff(sample_content, delimiters=[',', ';', '\t', '|']).delimiter
        except csv.Error:
            return ','

# ========================================
# UI COMPONENTS
# ========================================

def display_cleaning_stats(stats: dict):
    """Displays data cleaning statistics."""
    st.markdown("### 📊 Cleaning Statistics")
    col1, col2 = st.columns(2)
    col1.metric("Rows Removed", f"{stats['filas_eliminadas']:,}")
    col2.metric("Columns Removed", f"{stats['columnas_eliminadas']:,}")

# ========================================
# MAIN APPLICATION
# ========================================

def main():
    """Main application function."""
    initialize_session_state()
    apply_custom_styles()

    st.title("🧹 Data Cleaner and Processor")

    with st.sidebar:
        st.header("ℹ️ How to Use")
        st.markdown("1. **Upload** one or more CSV files.")
        st.markdown("2. Choose one of the processing options.")
        st.markdown("- **Clean & Download**: For quick, in-memory cleaning.")
        st.markdown("- **Process for Transformation**: To load large files into the database.")

    uploaded_files = st.file_uploader("Select CSV files", type=["csv", "txt"], accept_multiple_files=True)

    if uploaded_files:
        db_manager = DatabaseManager()
        file_processor = FileProcessor(db_manager)
        download_manager = DownloadManager()

        st.header("⚙️ Processing Options")
        col1, col2 = st.columns(2)

        with col1:
            if st.button("🧼 Clean & Download Files", use_container_width=True):
                with st.spinner("Cleaning files..."):
                    dataframes = [file_processor.process_regular_file(f) for f in uploaded_files]
                    valid_dfs = [df for df in dataframes if df is not None]
                    if valid_dfs:
                        combined_df = pd.concat(valid_dfs, ignore_index=True)
                        cleaned_df, stats = file_processor.clean_dataframe(combined_df)
                        st.session_state.cleaned_data = cleaned_df
                        st.session_state.cleaning_stats = stats
                    else:
                        st.error("No valid data could be processed.")

        with col2:
            if st.button("🚀 Process for Transformation", use_container_width=True):
                with st.spinner("Processing and loading to database..."):
                    success_count = 0
                    for file in uploaded_files:
                        if file_processor.process_large_file(file):
                            success_count += 1
                    if success_count > 0:
                        st.session_state.raw_data_loaded = True
                        st.success(f"Successfully processed {success_count} file(s) into the database.")
                    else:
                        st.error("No files were successfully processed.")
        
        if st.session_state.cleaning_stats:
            st.header("🎉 Cleaning Complete")
            display_cleaning_stats(st.session_state.cleaning_stats)
            st.dataframe(st.session_state.cleaned_data.head(100))
            
            st.header("📥 Download Cleaned Data")
            d_col1, d_col2 = st.columns(2)
            with d_col1:
                csv_link = download_manager.create_download_link(st.session_state.cleaned_data, "cleaned_data.csv", "Download CSV", "csv")
                st.markdown(csv_link, unsafe_allow_html=True)
            with d_col2:
                excel_link = download_manager.create_download_link(st.session_state.cleaned_data, "cleaned_data.xlsx", "Download Excel", "excel")
                st.markdown(excel_link, unsafe_allow_html=True)

        if st.session_state.raw_data_loaded:
            st.info("Data is now ready for transformation. Navigate to the 'Transformación' page.")

if __name__ == "__main__":
    main()

