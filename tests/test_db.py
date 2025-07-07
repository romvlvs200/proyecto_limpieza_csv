import pytest
from sqlalchemy.engine import Engine
from utils.db import get_engine


def test_get_engine_success():
    """Tests that the get_engine function returns a valid engine."""
    engine = get_engine()
    assert engine is not None
    assert isinstance(engine, Engine)

