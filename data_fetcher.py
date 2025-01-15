import pandas as pd
import streamlit as st

class Data_fetcher:

    def __init__(_self, con, env) -> None:
        _self.con = con
        _self.env = env

    @st.cache_data
    def fetch_data(_self, query: str) -> pd.DataFrame:

        try:
            cursor = _self.con.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
        except:
            raise
        finally:
           cursor.close()

        return pd.DataFrame(results, columns=column_names)
    
    def get_total_event_started(_self) -> pd.DataFrame:

        query = f'''
                select EVENT_NAME, count(*) AS EVENT_COUNT from ACCOUNT_EVENTS
                WHERE GAME_NAME = 'The Experience'
                AND ENVIRONMENT_NAME = '{_self.env}'
                AND event_name IN ('gameStarted','experienceStarted','perfectServingStarted','breweryIngredientsStarted','artOfBrewingStarted')
                GROUP BY EVENT_NAME
                ORDER BY EVENT_NAME DESC
                '''
        try:
            res = _self.fetch_data(query)
        except:
            raise
        
        return res
    
    def get_total_event_started_MOCK(_self) -> pd.DataFrame:
        
        # Mock data
        data = {
            'EVENT_NAME': [
                'perfectServingStarted',
                'gameStarted',
                'experienceStarted',
                'breweryIngredientsStarted',
                'artOfBrewingStarted'
            ],
            'EVENT_COUNT': [33, 96, 164, 29, 30]
        }

        return pd.DataFrame(data)
    
    def get_generic_session_durations(_self) -> pd.DataFrame:
        
        query = f'''
                WITH session_data AS (
                SELECT
                    EVENT_JSON:sessionID::STRING AS session_id,
                    DATE_TRUNC('day', event_timestamp) AS session_date,
                    MIN(event_timestamp) AS session_start_time,
                    MAX(event_timestamp) AS session_end_time
                FROM
                    ACCOUNT_EVENTS
                WHERE
                    GAME_NAME = 'The Experience'
                    AND ENVIRONMENT_NAME = 'testing'
                    AND EVENT_JSON:sessionID::STRING IS NOT NULL
                    AND event_timestamp >= DATEADD('month', -1, CURRENT_DATE)
                GROUP BY
                    EVENT_JSON:sessionID::STRING, DATE_TRUNC('day', event_timestamp)
                )
                SELECT
                    session_date,
                    DATEDIFF('minute', session_start_time, session_end_time) AS session_duration
                FROM
                    session_data
                WHERE
                    DATEDIFF('minute', session_start_time, session_end_time) > 0

                '''

        try:
            res = _self.fetch_data(query)
            # res['DURATION_MINUTES'] = pd.to_numeric(res['DURATION_MINUTES'], errors='coerce')
            return res
        except:
            raise


    def get_generic_session_durations_MOCK(_self) -> pd.DataFrame:

        data = {
            "SESSION_DATE": [
                    "2024-11-12 00:00:00.000",
                    "2024-11-12 00:00:00.000",
                    "2024-11-13 00:00:00.000",
                    "2024-11-14 00:00:00.000",
                    "2024-11-14 00:00:00.000",
                    "2024-11-14 00:00:00.000",
                    "2024-11-14 00:00:00.000",
                    "2024-11-14 00:00:00.000",
                    "2024-11-14 00:00:00.000",
                    "2024-11-14 00:00:00.000",
                    "2024-11-14 00:00:00.000",
                    "2024-11-19 00:00:00.000",
                    "2024-11-19 00:00:00.000",
                    "2024-11-19 00:00:00.000",
                    "2024-12-02 00:00:00.000",
            ],
            "SESSION_DURATION": [
                    10, 8, 4, 5, 2, 4, 4, 13, 13, 2, 6, 2, 2, 3, 12
            ]
        }
        df = pd.DataFrame(data)
        return df
    

    def get_event_count_by_device_token(_self):
        
        device_event_count_query = f'''
            SELECT DISTINCT
                event_name,
                EVENT_JSON:deviceName::STRING AS device_name,
                EVENT_JSON:deviceToken::STRING AS device_token,
                COUNT(*) AS event_count
            FROM
                account_events
            WHERE
                game_name = 'The Experience'
            AND
                environment_name = '{_self.env}'
            AND
                event_name in ('gameStarted','experienceStarted','perfectServingStarted','breweryIngredientsStarted','artOfBrewingStarted')
            
            AND  (
                EVENT_JSON:deviceToken::STRING IS NOT NULL 
                OR (
                    EVENT_JSON:deviceToken::STRING IS NULL 
                    AND EVENT_JSON:deviceName::STRING IS NULL
                )
            )
            GROUP BY
                event_name,
                EVENT_JSON:deviceName::STRING,
                EVENT_JSON:deviceToken::STRING
            ORDER BY
                event_name, event_count DESC;
            '''

        try:
            res = _self.fetch_data(device_event_count_query)

            res["DEVICE_NAME"] = res["DEVICE_NAME"].fillna("None")
            res["DEVICE_TOKEN"] = res["DEVICE_TOKEN"].fillna("None")

            device_mapping = res.groupby("DEVICE_NAME")["DEVICE_TOKEN"].first().reset_index()

            pivot_df = res.pivot_table(
                index="DEVICE_NAME",
                columns="EVENT_NAME",
                values="EVENT_COUNT",
                fill_value=0
            ).reset_index()

            pivot_df = pivot_df.merge(device_mapping, on="DEVICE_NAME", how="left")
            return pivot_df

        except:
            raise
            
    
    def get_event_count_by_device_token_MOCK(_self) -> pd.DataFrame:
        
        # Mock data
        data = {
            'DEVICE_TOKEN': [
                '75e850db523a96489225e3adf3e88ac7',
                None,
                'fd63c0bb12108eebfd0a79f9df782ab41beff88e'
            ],
            'artOfBrewingStarted': [11.0, 7.0, 12.0],
            'breweryIngredientsStarted': [8.0, 9.0, 12.0],
            'experienceStarted': [35.0, 93.0, 36.0],
            'gameStarted': [0.0, 96.0, 0.0],
            'perfectServingStarted': [6.0, 15.0, 12.0]
        }

        return pd.DataFrame(data)
    
    def get_latest_event_timestamps_by_devicetoken(_self, deviceToken: str) -> pd.DataFrame:

        device_token_dates_query = f'''
        SELECT
            EVENT_NAME,
            MAX(event_timestamp) AS latest_event_timestamp
        FROM
            account_events
        WHERE
            game_name = 'The Experience'
            AND environment_name = '{_self.env}'
            AND EVENT_JSON:deviceToken::STRING = '{deviceToken}'
            AND EVENT_NAME IN ('gameStarted', 'experienceStarted', 'perfectServingStarted', 'breweryIngredientsStarted', 'artOfBrewingStarted')
        GROUP BY
            EVENT_NAME
        ORDER BY
            latest_event_timestamp DESC;
        '''

        try:
            res = _self.fetch_data(device_token_dates_query)
            return res
        except:
            raise

    def get_session_durations_by_devicetoken(_self, deviceToken: str) -> pd.DataFrame:
        query = f'''
                WITH session_data AS (
                SELECT
                    EVENT_JSON:sessionID::STRING AS session_id,
                    DATE_TRUNC('day', event_timestamp) AS session_date,
                    MIN(event_timestamp) AS session_start_time,
                    MAX(event_timestamp) AS session_end_time
                FROM
                    ACCOUNT_EVENTS
                WHERE
                    GAME_NAME = 'The Experience'
                    AND ENVIRONMENT_NAME = '{_self.env}'
                    AND EVENT_JSON:deviceToken::STRING = '{deviceToken}'
                    AND EVENT_JSON:sessionID::STRING IS NOT NULL
                    AND event_timestamp >= DATEADD('month', -1, CURRENT_DATE)
                GROUP BY
                    EVENT_JSON:sessionID::STRING, DATE_TRUNC('day', event_timestamp)
            )
            SELECT
                session_date,
                DATEDIFF('minute', session_start_time, session_end_time) AS session_duration
            FROM
                session_data
            WHERE
                DATEDIFF('minute', session_start_time, session_end_time) > 0
            '''
        
        try:
            res = _self.fetch_data(query)
            return res
        except:
            raise

