import streamlit as st
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.connection import get_session


def render():
    st.header("Race Analytics Dashboard")

    session = get_session()

    # ── Filters ────────────────────────────────────────────────────────────────
    with st.sidebar:
        st.subheader("Filters")

        years = session.sql(
            "SELECT DISTINCT year FROM APEXML_DB.PROD.DIM_SESSIONS ORDER BY year DESC"
        ).to_pandas()["YEAR"].tolist()
        selected_year = st.selectbox("Year", years)

        sessions_df = session.sql(f"""
            SELECT session_key, meeting_name || ' — ' || session_name AS label
            FROM APEXML_DB.PROD.DIM_SESSIONS
            WHERE year = {selected_year}
            ORDER BY session_start_at
        """).to_pandas()
        session_labels = sessions_df["LABEL"].tolist()
        selected_label = st.selectbox("Session", session_labels)
        selected_session_key = int(
            sessions_df[sessions_df["LABEL"] == selected_label]["SESSION_KEY"].iloc[0]
        )

        drivers_df = session.sql(f"""
            SELECT DISTINCT driver_number, full_name
            FROM APEXML_DB.PROD.DIM_DRIVERS
            WHERE session_key = {selected_session_key}
            ORDER BY full_name
        """).to_pandas()
        driver_options = drivers_df["FULL_NAME"].tolist()
        selected_drivers = st.multiselect("Drivers", driver_options, default=driver_options[:5])
        selected_driver_numbers = drivers_df[
            drivers_df["FULL_NAME"].isin(selected_drivers)
        ]["DRIVER_NUMBER"].tolist()

    if not selected_drivers:
        st.warning("Select at least one driver.")
        return

    driver_filter = ", ".join(str(d) for d in selected_driver_numbers)

    # ── Metrics ────────────────────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)

    laps_count = session.sql(f"""
        SELECT COUNT(*) AS n FROM APEXML_DB.PROD.FCT_LAPS
        WHERE session_key = {selected_session_key}
    """).to_pandas()["N"].iloc[0]

    fastest_lap = session.sql(f"""
        SELECT MIN(lap_duration_s) AS fl FROM APEXML_DB.PROD.FCT_LAPS
        WHERE session_key = {selected_session_key}
    """).to_pandas()["FL"].iloc[0]

    pit_count = session.sql(f"""
        SELECT COUNT(*) AS n FROM APEXML_DB.PROD.FCT_PIT_STOPS
        WHERE session_key = {selected_session_key}
    """).to_pandas()["N"].iloc[0]

    col1.metric("Total Laps", laps_count)
    col2.metric("Fastest Lap", f"{fastest_lap:.3f}s" if fastest_lap else "N/A")
    col3.metric("Pit Stops", pit_count)
    col4.metric("Drivers", len(selected_drivers))

    st.divider()

    # ── Charts ─────────────────────────────────────────────────────────────────
    chart1, chart2 = st.columns(2)

    # Lap times per driver
    with chart1:
        st.subheader("Lap Times")
        lap_data = session.sql(f"""
            SELECT lap_number, lap_duration_s, driver_name, tyre_compound
            FROM APEXML_DB.PROD.FCT_LAPS
            WHERE session_key = {selected_session_key}
              AND driver_number IN ({driver_filter})
            ORDER BY lap_number
        """).to_pandas()

        if not lap_data.empty:
            import altair as alt
            chart = alt.Chart(lap_data).mark_line(point=True).encode(
                x=alt.X("LAP_NUMBER:Q", title="Lap"),
                y=alt.Y("LAP_DURATION_S:Q", title="Lap Time (s)", scale=alt.Scale(zero=False)),
                color="DRIVER_NAME:N",
                tooltip=["DRIVER_NAME", "LAP_NUMBER", "LAP_DURATION_S", "TYRE_COMPOUND"],
            ).properties(height=350)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No lap data available.")

    # Race positions
    with chart2:
        st.subheader("Race Positions")
        pos_data = session.sql(f"""
            SELECT date AS ts, position, driver_number
            FROM APEXML_DB.PROD.FCT_RACE_POSITIONS
            WHERE session_key = {selected_session_key}
              AND driver_number IN ({driver_filter})
            ORDER BY ts
        """).to_pandas()

        if not pos_data.empty:
            # Join driver names
            pos_data = pos_data.merge(
                drivers_df[drivers_df["DRIVER_NUMBER"].isin(selected_driver_numbers)],
                left_on="DRIVER_NUMBER",
                right_on="DRIVER_NUMBER",
                how="left"
            )
            import altair as alt
            chart = alt.Chart(pos_data).mark_line().encode(
                x=alt.X("TS:T", title="Time"),
                y=alt.Y("POSITION:Q", title="Position", scale=alt.Scale(reverse=True)),
                color="FULL_NAME:N",
                tooltip=["FULL_NAME", "POSITION", "TS"],
            ).properties(height=350)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No position data available.")

    chart3, chart4 = st.columns(2)

    # Pit stops
    with chart3:
        st.subheader("Pit Stops")
        pit_data = session.sql(f"""
            SELECT driver_name, lap_number, pit_duration_s, tyre_compound_out, tyre_compound_in
            FROM APEXML_DB.PROD.FCT_PIT_STOPS
            WHERE session_key = {selected_session_key}
              AND driver_number IN ({driver_filter})
            ORDER BY lap_number
        """).to_pandas()

        if not pit_data.empty:
            st.dataframe(pit_data, use_container_width=True)
        else:
            st.info("No pit stop data.")

    # Tyre stints
    with chart4:
        st.subheader("Tyre Stints")
        stint_data = session.sql(f"""
            SELECT driver_name, stint_number, tyre_compound, lap_start, lap_end,
                   stint_length, avg_lap_time_s, fastest_lap_s
            FROM APEXML_DB.PROD.FCT_STINTS
            WHERE session_key = {selected_session_key}
              AND driver_number IN ({driver_filter})
            ORDER BY driver_name, stint_number
        """).to_pandas()

        if not stint_data.empty:
            st.dataframe(stint_data, use_container_width=True)
        else:
            st.info("No stint data.")
