import streamlit as st
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import snowflake.connector as con
import plotly.express as px
from st_aggrid import AgGrid, GridOptionsBuilder
import plotly.graph_objects as go
import re
import data_fetcher

st.set_page_config(layout='wide')

plotly_template = "plotly_dark"

header = st.container(border=True)
body = st.container(border=False)

if "selected_row" not in st.session_state:
    st.session_state.selected_row = None

if "isActive" not in st.session_state:
    st.session_state.isActive = False

if "fetcher" not in st.session_state:
    st.session_state.fetcher = None


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

def fetch_data(fetcher):
    with ThreadPoolExecutor() as executor:
        future_total_count = executor.submit(fetcher.get_total_event_started)
        future_session_dur = executor.submit(fetcher.get_generic_session_durations)
        future_event_count = executor.submit(fetcher.get_event_count_by_device_token)

        return (
            future_total_count.result(),
            future_session_dur.result(),
            future_event_count.result()
        )
    
def validate_input_string(input: str):
    pattern = "^[a-zA-Z]+$"
    return re.match(pattern, input) is not None

def main():

    with header:
        st.title('Virsabi analytics for "The Experience" [Alpha]')
        user = st.text_input("Username: ")
        key = st.text_input("Key: ", type="password")
        env = st.text_input("Envrironment: ")

        if st.button("Fetch Data"):
            try:
                if not validate_input_string(env):
                    st.error("Environments can only be a-Z characters")
                    return
                con = get_snowflake_connection(user, key)
                fetcher = data_fetcher.Data_fetcher(con, env)
                st.session_state.fetcher = fetcher
                st.session_state.isActive = True
            except Exception as e:
                print(f"Login error: {e}")
                st.error("Credential Error!")
                return

             

    with body:

        col_l, col_r = st.columns(2, gap='medium')

        if st.session_state.isActive:

            with st.spinner("Fetching data... (First time may take several minutes)"):
                st.session_state.isActive = True
                df_total_count, df_session_dur, df_event_count_by_device = fetch_data(st.session_state.fetcher)

            with col_l:
                with st.container(key="col_container", border=True):
                    st.header("Total Metrics")
                    if df_total_count is not None:
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


                    if df_session_dur is not None:

                        df_daily_avg = (
                            df_session_dur.groupby('SESSION_DATE')['SESSION_DURATION']
                            .mean()
                            .reset_index()
                            .rename(columns={'SESSION_DURATION': 'AVG_SESSION_DURATION'})
                        )

                        fig_avg_daily = px.line(
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

                        fig_metrics = px.bar(
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


            with col_r:
                with st.container(key="col_container2", border=True):

                    st.header("Metrics by device")

                    if df_event_count_by_device is not None:
                        st.write("Device Event Table. Select row for visualization")     
                        grid_options = GridOptionsBuilder.from_dataframe(df_event_count_by_device)
                        grid_options.configure_selection('single')  # Single-row selection mode
                        grid_options = grid_options.build()

                        response = AgGrid(
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

                        if st.session_state.selected_row is not None:
                            print("session row found")
                            row = df_event_count_by_device.iloc[[st.session_state.selected_row]]
                            event_data = {
                                'EVENT_NAME': list(df_event_count_by_device.columns[1:]),
                                'EVENT_COUNT': [row.get(col) for col in df_event_count_by_device.columns[1:]]
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
                        
                    

if __name__ == "__main__":
    main()