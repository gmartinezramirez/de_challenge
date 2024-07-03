import pandas as pd
import pytest

from q1_memory import process_chunk, q1_memory

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


def test_q1_memory_integration():
    """Test de integracion usando un dataset de 20 registros random
    Q1: Las top 10 fechas donde hay más tweets.
    Mencionar el usuario (username) que más publicaciones tiene por cada uno de esos días.
    Ejemplo de retorno:
        [(datetime.date(1999, 11, 15), "LATAM321"), (datetime.date(1999, 7, 15), "LATAM_CHI"), ...]
    """

    df = pd.DataFrame(MOCK_DATASET)

    def mock_read_json(*args, **kwargs):
        return df

    with pytest.patch("pandas.read_json", mock_read_json):
        result = q1_memory("fake_file.json")

    # Verificar los resultados
    assert len(result) == 0  # TODO


@pytest.mark.parametrize("file_path", ["non_existent_file.json"])
def test_q1_memory_file_not_found(file_path):
    with pytest.raises(FileNotFoundError):
        q1_memory(file_path)
