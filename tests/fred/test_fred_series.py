import pytest

from finstream.fred.series import INITIAL_FRED_SERIES_IDS


def test_initial_fred_series_ids_have_exact_order() -> None:
    assert INITIAL_FRED_SERIES_IDS == (
        "DFF",
        "CPIAUCSL",
        "UNRATE",
        "GDPC1",
        "DGS10",
    )


def test_initial_fred_series_ids_are_unique() -> None:
    assert len(INITIAL_FRED_SERIES_IDS) == len(set(INITIAL_FRED_SERIES_IDS))


def test_initial_fred_series_configuration_is_immutable() -> None:
    assert isinstance(INITIAL_FRED_SERIES_IDS, tuple)
    with pytest.raises(TypeError):
        INITIAL_FRED_SERIES_IDS[0] = "OTHER"  # type: ignore[index]


def test_initial_fred_series_ids_are_non_blank_strings() -> None:
    assert all(
        isinstance(series_id, str) and series_id.strip()
        for series_id in INITIAL_FRED_SERIES_IDS
    )
