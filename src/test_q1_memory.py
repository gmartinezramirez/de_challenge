import json
import unittest.mock
from collections import defaultdict
from datetime import date

import pandas as pd
import pytest

from .q1_memory import get_top_10_dates, get_top_users, process_chunk, q1_memory

# Dataset con 20 registros random
MOCK_DATASET = [
    {"date": "2021-02-01", "user": {"username": "user1"}},
    {"date": "2021-02-01", "user": {"username": "user2"}},
    {"date": "2021-02-01", "user": {"username": "user1"}},
    {"date": "2021-02-02", "user": {"username": "user3"}},
    {"date": "2021-02-02", "user": {"username": "user2"}},
    {"date": "2021-02-03", "user": {"username": "user4"}},
    {"date": "2021-02-03", "user": {"username": "user1"}},
    {"date": "2021-02-03", "user": {"username": "user2"}},
    {"date": "2021-02-04", "user": {"username": "user5"}},
    {"date": "2021-02-04", "user": {"username": "user3"}},
    {"date": "2021-02-05", "user": {"username": "user1"}},
    {"date": "2021-02-05", "user": {"username": "user2"}},
    {"date": "2021-02-05", "user": {"username": "user3"}},
    {"date": "2021-02-06", "user": {"username": "user4"}},
    {"date": "2021-02-06", "user": {"username": "user5"}},
    {"date": "2021-02-07", "user": {"username": "user1"}},
    {"date": "2021-02-07", "user": {"username": "user2"}},
    {"date": "2021-02-07", "user": {"username": "user3"}},
    {"date": "2021-02-07", "user": {"username": "user4"}},
    {"date": "2021-02-07", "user": {"username": "user5"}},
]


@pytest.fixture
def sample_df():
    return pd.DataFrame(MOCK_DATASET)


def test_process_chunk(sample_df):
    date_counts, date_user_counts = process_chunk(sample_df)

    assert date_counts[date(2021, 2, 1)] == 3
    assert date_counts[date(2021, 2, 7)] == 5

    assert date_user_counts[date(2021, 2, 1)]["user1"] == 2
    assert date_user_counts[date(2021, 2, 7)]["user5"] == 1


def test_get_top_10_dates():
    date_counts = defaultdict(
        int,
        {
            date(2021, 2, 1): 3,
            date(2021, 2, 2): 2,
            date(2021, 2, 3): 3,
            date(2021, 2, 4): 2,
            date(2021, 2, 5): 3,
            date(2021, 2, 6): 2,
            date(2021, 2, 7): 5,
        },
    )

    top_dates = get_top_10_dates(date_counts)

    assert len(top_dates) == 7  # Hay 7 fechas en total
    assert top_dates[0] == (date(2021, 2, 7), 5)
    assert set(date for date, count in top_dates if count == 2) == {
        date(2021, 2, 2),
        date(2021, 2, 4),
        date(2021, 2, 6),
    }


def test_get_top_users():
    top_dates = [(date(2021, 2, 7), 5), (date(2021, 2, 1), 3), (date(2021, 2, 5), 3)]
    date_user_counts = defaultdict(
        lambda: defaultdict(int),
        {
            date(2021, 2, 7): {
                "user1": 1,
                "user2": 1,
                "user3": 1,
                "user4": 1,
                "user5": 1,
            },
            date(2021, 2, 1): {"user1": 2, "user2": 1},
            date(2021, 2, 5): {"user1": 1, "user2": 1, "user3": 1},
        },
    )

    top_users = get_top_users(top_dates, date_user_counts)

    assert len(top_users) == 3
    assert top_users[0] == (
        date(2021, 2, 7),
        "user1",
    )  # Cualquiera de los usuarios podría ser el primero
    assert top_users[1] == (date(2021, 2, 1), "user1")
    assert top_users[2] == (
        date(2021, 2, 5),
        "user1",
    )  # Cualquiera de los usuarios podría ser el primero


def test_q1_memory_integration():
    """Test de integracion usando un dataset de 20 registros random"""

    df = pd.DataFrame(MOCK_DATASET)

    def mock_read_json(*args, **kwargs):
        return df

    with unittest.mock.patch("pandas.read_json", mock_read_json):
        result = q1_memory("fake_file.json")

    # Verificar los resultados
    assert len(result) == 7  # Debería haber 7 fechas únicas en nuestros datos de prueba
    assert result[0] == (
        date(2021, 2, 7),
        "user1",
    )  # La fecha con más tweets (5) y un user más activo
    assert result[1][0] in {
        date(2021, 2, 1),
        date(2021, 2, 3),
        date(2021, 2, 5),
    }  # Fechas con 3 tweets
    assert result[2][0] in {
        date(2021, 2, 1),
        date(2021, 2, 3),
        date(2021, 2, 5),
    }  # Fechas con 3 tweets
    assert result[3][0] in {
        date(2021, 2, 1),
        date(2021, 2, 3),
        date(2021, 2, 5),
    }  # Fechas con 3 tweets


@pytest.mark.parametrize("file_path", ["non_existent_file.json"])
def test_q1_memory_file_not_found(file_path):
    with pytest.raises(FileNotFoundError):
        q1_memory(file_path)
