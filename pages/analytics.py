# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session


# Get the current credentials
session = get_active_session()

st.title("Analytics")      
flow_df = st.session_state.flow_df

st.line_chart(flow_df.groupby('date').size(), use_container_width=True)
st.bar_chart(flow_df.groupby('GAME_ID').size() ,use_container_width=True)
