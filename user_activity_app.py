# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session



# Get the current credentials
session = get_active_session()

st.title("User Tracking")

@st.cache_data
def user_activity(selected_user,start,end):
    fetch_sql = f"""
    select *
    from quickstart.gaming_anomaly.raw_events
    where user_id = '{selected_user}' and event_time between '{start}' and '{end}'
    order by event_time asc
    """
    flow_df = session.sql(fetch_sql).to_pandas()
    return flow_df


@st.cache_data
def users():
    user_list = session.sql("SELECT DISTINCT USER_ID FROM quickstart.gaming_anomaly.raw_events").to_pandas()
    return user_list


users_list = users()

col1,col2,col3,col4 = st.columns(4)
selected_user = col1.selectbox("User",users_list, index=None)
start = col2.date_input("Start Date", value=None)
end = col3.date_input("End Date", value=None)
col4.write(' ')
col4.write(' ')

submit = col4.button("APPLY", type = 'primary')

if submit:
    if end < start:
        st.error("End Date must be after Start Date")
    else:
        flow_df = user_activity(selected_user,start,end)
        flow_df['date'] = flow_df['EVENT_TIME'].dt.date

        st.dataframe(flow_df)
        st.line_chart(flow_df.groupby('date').size())