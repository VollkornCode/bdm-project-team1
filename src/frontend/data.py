from __future__ import annotations

from pathlib import Path
import sys
from typing import Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.conf import DELTALAKE_TABLES
from scripts.ingest import DeltaLakeClient

FAOSTAT_EXPLOITATION_PATH = DELTALAKE_TABLES["FAOSTAT_FOOD_CPI_EXPLOITATION"]
VALUE_COLUMN_CANDIDATES = ("avg_value", "Value", "value", "cpi_value")
COUNTRY_COLUMN_CANDIDATES = ("Country", "country", "Area", "area")
YEAR_COLUMN_CANDIDATES = ("Year", "year")


def load_faostat_exploitation_frame() -> pd.DataFrame:
    client = DeltaLakeClient()
    frame = client.read_table(FAOSTAT_EXPLOITATION_PATH)
    return frame.to_pandas()


def resolve_column(columns: Iterable[str], candidates: tuple[str, ...]) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    raise ValueError(f"Could not resolve any of {candidates} in dataframe columns: {list(columns)}")


def standardize_faostat_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()

    country_column = resolve_column(frame.columns, COUNTRY_COLUMN_CANDIDATES)
    year_column = resolve_column(frame.columns, YEAR_COLUMN_CANDIDATES)
    value_column = resolve_column(frame.columns, VALUE_COLUMN_CANDIDATES)

    standardized = frame[[country_column, year_column, value_column]].copy()
    standardized = standardized.rename(
        columns={
            country_column: "Country",
            year_column: "Year",
            value_column: "Value",
        }
    )

    standardized["Country"] = standardized["Country"].astype(str)
    standardized["Year"] = pd.to_numeric(standardized["Year"], errors="coerce")
    standardized["Value"] = pd.to_numeric(standardized["Value"], errors="coerce")
    standardized = standardized.dropna(subset=["Country", "Year", "Value"])
    standardized["Year"] = standardized["Year"].astype(int)

    return standardized


def available_countries(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return []
    return sorted(frame["Country"].dropna().astype(str).unique().tolist())


def normalize_faostat_by_baseline(
    frame: pd.DataFrame,
    baseline_year: int = 2015,
) -> tuple[pd.DataFrame, list[str]]:
    if frame.empty:
        return frame.copy(), []

    normalized_frames: list[pd.DataFrame] = []
    skipped_countries: list[str] = []

    for country, country_frame in frame.groupby("Country", sort=True):
        baseline_rows = country_frame.loc[country_frame["Year"] == baseline_year, "Value"]
        if baseline_rows.empty:
            skipped_countries.append(country)
            continue

        baseline_value = float(baseline_rows.mean())
        if baseline_value == 0:
            skipped_countries.append(country)
            continue

        country_copy = country_frame.sort_values("Year").copy()
        country_copy["Normalized Value"] = (country_copy["Value"] / baseline_value) * 100
        normalized_frames.append(country_copy)

    if not normalized_frames:
        return pd.DataFrame(columns=["Country", "Year", "Value", "Normalized Value"]), skipped_countries

    normalized = pd.concat(normalized_frames, ignore_index=True)
    normalized = normalized.sort_values(["Country", "Year"])
    return normalized, skipped_countries
