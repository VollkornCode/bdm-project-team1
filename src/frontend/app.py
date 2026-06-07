from __future__ import annotations

from pathlib import Path
import sys

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from frontend.tabs.faostat import render_faostat_tab


st.set_page_config(
    page_title="FAOSTAT Explorer",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.title("FAOSTAT Consumption Explorer")
st.write("A lightweight Delta-backed frontend for comparing CPI development across countries.")

faostat_tab, future_tab = st.tabs(["FAOSTAT CPI", "Future tabs"])

with faostat_tab:
    render_faostat_tab()

with future_tab:
    st.info("Add the next consumption view here without changing the FAOSTAT tab.")
