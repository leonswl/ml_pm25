import requests

import streamlit as st

from settings import API_URL, TITLE
from components import build_metrics_plot, build_data_plot


st.set_page_config(page_title=TITLE)
st.title(TITLE)

# Create plot for metrics over time.
st.plotly_chart(build_metrics_plot())

st.divider()

st.plotly_chart(build_data_plot())