import streamlit as st
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import snowflake.connector as con
import plotly.express as px
from st_aggrid import AgGrid, GridOptionsBuilder
import re
import data_fetcher


st.set_page_config(layout='wide')

plotly_template = "plotly_dark"

header = st.container(border=True)
body = st.container(border=False)

# Data to be persistent across restarts
if "selected_row" not in st.session_state:
    st.session_state.selected_row = None

if "isActive" not in st.session_state:
    st.session_state.isActive = False

if "fetcher" not in st.session_state:
    st.session_state.fetcher = None

if "clear_cache" not in st.session_state:
    st.session_state.clearCache = False


def main():
    
    with header: # Login input header
        st.title('Virsabi analytics for "The Experience" [Alpha]')
        user = st.text_input("Username: ")
        key = st.text_input("API Key: ", type="password")
        env = st.text_input("Envrironment: ")

        if st.button("Fetch Data"):
            try:
                if not validate_input_string(env):
                    st.error("Environments can only be a-Z characters")
                    return
                con = get_snowflake_connection(user, key)
                fetcher = data_fetcher.Data_fetcher(con, env)
                st.session_state.clear_cache = False
                st.session_state.fetcher = fetcher
                st.session_state.isActive = True
            except Exception as e:
                print(f"Login error: {e}")
                st.error("Credential Error!")
                return
            
        if "fetcher" in st.session_state and st.button("Clear Cache"):
            st.cache_data.clear()  # Clear the cache
            st.session_state.isActive = False  # Deactivate data display
            st.session_state.clear_cache = True  # Signal that the cache was cleared

             

    with body: # Data visualization fields

        col_l, col_r = st.columns(2, gap='medium')

        if st.session_state.isActive:
            if st.session_state.get("clear_cache", False):
                st.warning("Cache has been cleared. Please reload data.")

            else:
                with st.spinner("Fetching data... (First time may take several minutes)"):
                    st.session_state.isActive = True
                    df_total_count, df_session_dur, df_event_count_by_device = None, None, None
                    df_total_count, df_session_dur, df_event_count_by_device = run_funcs_async(st.session_state.fetcher.get_total_event_started,
                                                                                            st.session_state.fetcher.get_generic_session_durations,
                                                                                            st.session_state.fetcher.get_event_count_by_device_token)

                with col_l: # Left column with total data
                    with st.container(key="col_container", border=True):
                        st.header("Total Metrics")
                        if df_total_count is not None: # Total event count dataframe
                            fig = px.bar(
                                df_total_count, 
                                x='EVENT_NAME', 
                                y='EVENT_COUNT', 
                                title="Total Event Counts for 'The Experience'", 
                                labels={'EVENT_NAME': 'Event Name', 'EVENT_COUNT': 'Event Count'}, 
                                text='EVENT_COUNT'  # Show count values on the bars
                                )

                            fig.update_layout(
                                xaxis_title="Event Name",
                                yaxis_title="Count",
                                template=plotly_template
                            )
                            st.plotly_chart(fig)


                        if df_session_dur is not None: # Session duration dataframe. used for next 2 charts

                            df_daily_avg = (
                                df_session_dur.groupby('SESSION_DATE')['SESSION_DURATION']
                                .mean()
                                .reset_index()
                                .rename(columns={'SESSION_DURATION': 'AVG_SESSION_DURATION'})
                            )

                            fig_avg_daily = px.line( # Average duration line chart
                            df_daily_avg,
                            x='SESSION_DATE',
                            y='AVG_SESSION_DURATION',
                            title='Average Session Duration Per Day (Last Month)',
                            labels={'SESSION_DATE': 'Date', 'AVG_SESSION_DURATION': 'Average Duration (minutes)'},
                            markers=True
                            )
                            fig_avg_daily.update_layout(template='plotly_white')
                            st.plotly_chart(fig_avg_daily)

                            metrics = {
                                'Shortest': df_session_dur['SESSION_DURATION'].min(),
                                'Longest': df_session_dur['SESSION_DURATION'].max(),
                                'Average': df_session_dur['SESSION_DURATION'].mean()
                            }
                            df_metrics = pd.DataFrame(
                                metrics.items(),
                                columns=['DURATION_METRIC', 'DURATION_MINUTES']
                            )

                            fig_metrics = px.bar( # aggregated metrics bar chart
                            df_metrics,
                            x='DURATION_MINUTES',
                            y='DURATION_METRIC',
                            text='DURATION_MINUTES',
                            orientation='h',  # Horizontal orientation
                            title='Session Duration Metrics (Last Month)',
                            labels={'DURATION_METRIC': 'Duration Metric', 'DURATION_MINUTES': 'Minutes'},
                            color='DURATION_METRIC',
                            color_discrete_sequence=px.colors.qualitative.Set2
                            )
                            fig_metrics.update_traces(texttemplate='%{text:.2f}', textposition='outside')
                            fig_metrics.update_layout(
                                xaxis_title='Minutes',
                                yaxis_title='Duration Metric',
                                template=plotly_template,
                                showlegend=False
                            )

                            st.plotly_chart(fig_metrics)


                with col_r: # Right column for data by device
                    with st.container(key="col_container2", border=True):

                        st.header("Metrics by device")

                        if df_event_count_by_device is not None: # event count by device dataframe
                            st.write("Device Event Table. Select row for visualization")     
                            grid_options = GridOptionsBuilder.from_dataframe(df_event_count_by_device)
                            grid_options.configure_selection('single')  # Single-row selection mode
                            grid_options = grid_options.build()

                            response = AgGrid( # Interactive table
                                df_event_count_by_device,
                                gridOptions=grid_options,
                                height=300,
                                allow_unsafe_jscode=True,
                                update_mode='MODEL_CHANGED'
                            )

                            if response:
                                selected_row = response['selected_rows']

                            if selected_row is not None:
                                st.session_state.selected_row = selected_row.index[0]
                                print(f"session row update: {selected_row.index[0]}")

                            if st.session_state.selected_row is not None: # show extra device viz when row is selected
                                row = df_event_count_by_device.iloc[[st.session_state.selected_row]]
                                event_data = {
                                    'EVENT_NAME': list(df_event_count_by_device.columns[1:-1]),
                                    'EVENT_COUNT': [row.get(col) for col in df_event_count_by_device.columns[1:-1]]
                                }
                                event_df = pd.DataFrame(event_data)
                                event_df["EVENT_COUNT"] = event_df["EVENT_COUNT"].astype(int)
                                fig_title = row.get('DEVICE_NAME').to_string()
                                fig = px.bar(
                                    event_df,
                                    x='EVENT_NAME',
                                    y='EVENT_COUNT',
                                    title=f"Event Data for Device: {fig_title}",
                                    labels={'EVENT_NAME': 'Event Name', 'EVENT_COUNT': 'Event Count'},
                                    color='EVENT_NAME',
                                    color_discrete_sequence=px.colors.qualitative.Set2
                                )
                                fig.update_layout(
                                    xaxis_title="Event Names", yaxis_title="Event Counts",
                                    template=plotly_template
                                )
                                
                                st.plotly_chart(fig)

                                if (st.button("Fetch more")): # fetch extra device data from db when pressed
                                    with st.spinner("Fetching device data... May take several minutes first time"):
                                        st.write("Latest event recordings by device")
                                        token = str(row.iloc[0]["DEVICE_TOKEN"])
                                        results = run_funcs_async(
                                            st.session_state.fetcher.get_latest_event_timestamps_by_devicetoken,
                                            st.session_state.fetcher.get_session_durations_by_devicetoken,
                                            arg=token
                                            )

                                    device_timestamp_df = results[0]
                                    device_session_dur_df = results[1]
                                    st.write(device_timestamp_df) # show table
                            
                                    df_device_daily_avg = (
                                        device_session_dur_df.groupby('SESSION_DATE')['SESSION_DURATION']
                                        .mean()
                                        .reset_index()
                                        .rename(columns={'SESSION_DURATION': 'AVG_SESSION_DURATION'})
                                    )

                                    fig_avg_daily = px.line( # show line chart
                                    df_device_daily_avg,
                                    x='SESSION_DATE',
                                    y='AVG_SESSION_DURATION',
                                    title='Average Session Duration Per Day By Device (Last Month)',
                                    labels={'SESSION_DATE': 'Date', 'AVG_SESSION_DURATION': 'Average Duration (minutes)'},
                                    markers=True
                                    )
                                    fig_avg_daily.update_layout(template='plotly_white')
                                    st.plotly_chart(fig_avg_daily)
        st.write("Version 0.4")

@st.cache_resource
def get_snowflake_connection(i_user: str, key: str):
    try:
        conn = con.connect(
            account=st.secrets["account"],
            user=i_user,
            password=key
        )
        return conn 
    
    except:
        raise

def run_funcs_async(*functions, arg=None): # run several queries at once with optional argument
    results = [None] * len(functions)
    with ThreadPoolExecutor() as executor:

        if arg is None:
            future_to_index = {executor.submit(func): i for i, func in enumerate(functions)}
        else:
            future_to_index = {executor.submit(func, arg): i for i, func in enumerate(functions)}
            
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            try:
                results[index] = future.result()
            except Exception as e:
                results[index] = e

        return results


def validate_input_string(input: str): # only english letters allowed in env for SQL sanitation!
    pattern = "^[a-zA-Z]+$"
    return re.match(pattern, input) is not None

if __name__ == "__main__":
    main()