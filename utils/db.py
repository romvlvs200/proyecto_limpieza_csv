import time
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
import streamlit as st
from config import MAX_DB_RETRIES

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from utils.db_credentials import USER, PASSWORD, HOST, PORT, DB

DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"

@st.cache_resource
def get_engine():
    """
    Creates a reusable SQLAlchemy engine with a retry mechanism.
    """
    for attempt in range(MAX_DB_RETRIES):
        try:
            engine = create_engine(
                DATABASE_URL,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                echo=False
            )
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection successful.")
            return engine
        except OperationalError as e:
            logger.warning(f"Database connection attempt {attempt + 1} failed: {e}")
            if attempt < MAX_DB_RETRIES - 1:
                time.sleep(2) # Wait before retrying
            else:
                st.error("Database connection failed after several retries.")
                return None
    return None

def get_session():
    """Creates a new SQLAlchemy session."""
    engine = get_engine()
    if engine:
        Session = sessionmaker(bind=engine)
        return Session()
    return None
