from __future__ import annotations

import plotly.express as px
import streamlit as st

from frontend.data import (
    available_countries,
    load_faostat_exploitation_frame,
    normalize_faostat_by_baseline,
    standardize_faostat_frame,
)


@st.cache_data(ttl=300, show_spinner=False)
def load_chart_data():
    frame = load_faostat_exploitation_frame()
    standardized = standardize_faostat_frame(frame)
    countries = available_countries(standardized)
    normalized, skipped = normalize_faostat_by_baseline(standardized, baseline_year=2015)
    return standardized, countries, normalized, skipped


def render_faostat_tab() -> None:
    st.subheader("FAOSTAT CPI comparison")
    st.caption("Select one or more countries and compare their CPI development, normalized to 100 at 2015.")

    try:
        standardized, countries, normalized, skipped = load_chart_data()
    except Exception as exc:
        st.error(f"Could not load FAOSTAT exploitation data: {exc}")
        return

    if standardized.empty:
        st.info("No FAOSTAT exploitation data found yet.")
        return

    default_selection = countries[:3] if len(countries) >= 3 else countries
    selected_countries = st.multiselect(
        "Countries",
        options=countries,
        default=default_selection,
        placeholder="Select countries to compare",
    )

    if not selected_countries:
        st.info("Select at least one country to display the chart.")
        return

    selected = normalized.loc[normalized["Country"].isin(selected_countries)].copy()
    if selected.empty:
        st.warning("None of the selected countries had a 2015 baseline, so there is nothing to plot.")
        return

    missing_baseline = [country for country in selected_countries if country in skipped]
    if missing_baseline:
        st.warning(
            "These selected countries were skipped because no 2015 baseline was found: "
            + ", ".join(missing_baseline)
        )

    fig = px.line(
        selected.sort_values(["Country", "Year"]),
        x="Year",
        y="Normalized Value",
        color="Country",
        markers=True,
        title="FAOSTAT CPI normalized to 100 at 2015",
        labels={
            "Year": "Year",
            "Normalized Value": "Index (2015 = 100)",
            "Country": "Country",
        },
    )
    fig.update_layout(legend_title_text="Country", hovermode="x unified")

    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Show normalized data"):
        st.dataframe(selected, use_container_width=True, hide_index=True)
