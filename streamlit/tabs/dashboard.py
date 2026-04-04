import streamlit as st
import altair as alt
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.colors import get_driver_colors, altair_color_scale

TYRE_COLORS = {
    "SOFT": "#E8002D",
    "MEDIUM": "#FFF200",
    "HARD": "#FFFFFF",
    "INTERMEDIATE": "#39B54A",
    "WET": "#0067FF",
}


def render(session):
    st.header("Analytics")

    with st.sidebar:
        st.subheader("Filters")

        years = session.sql(
            "SELECT DISTINCT year FROM APEXML_DB.PROD.DIM_SESSIONS ORDER BY year DESC"
        ).to_pandas()["YEAR"].tolist()
        selected_years = st.multiselect("Years", years, default=years[:1])

        if not selected_years:
            st.warning("Select at least one year.")
            return

        year_filter = ", ".join(str(y) for y in selected_years)
        sessions_df = session.sql(f"""
            SELECT session_key, meeting_name || ' — ' || session_name AS label
            FROM APEXML_DB.PROD.DIM_SESSIONS
            WHERE year IN ({year_filter})
            ORDER BY session_start_at
        """).to_pandas()

        session_labels = sessions_df["LABEL"].tolist()
        selected_labels = st.multiselect("Sessions", session_labels, default=session_labels[:1])

        if not selected_labels:
            st.warning("Select at least one session.")
            return

        selected_keys = sessions_df[
            sessions_df["LABEL"].isin(selected_labels)
        ]["SESSION_KEY"].tolist()
        session_filter = ", ".join(str(k) for k in selected_keys)

        drivers_df = session.sql(f"""
            SELECT DISTINCT driver_number, full_name, team_name
            FROM APEXML_DB.PROD.DIM_DRIVERS
            WHERE session_key IN ({session_filter})
            ORDER BY full_name
        """).to_pandas()

        driver_options = drivers_df["FULL_NAME"].drop_duplicates().tolist()
        selected_drivers = st.multiselect("Drivers", driver_options, default=driver_options[:5])

        if not selected_drivers:
            st.warning("Select at least one driver.")
            return

        selected_driver_numbers = drivers_df[
            drivers_df["FULL_NAME"].isin(selected_drivers)
        ]["DRIVER_NUMBER"].drop_duplicates().tolist()
        driver_filter = ", ".join(str(d) for d in selected_driver_numbers)

    color_map = get_driver_colors(
        drivers_df[drivers_df["FULL_NAME"].isin(selected_drivers)].drop_duplicates("FULL_NAME")
    )
    color_scale = altair_color_scale(color_map)

    # ── Summary Metrics ────────────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)
    laps_count = session.sql(f"""
        SELECT COUNT(*) AS n FROM APEXML_DB.PROD.FCT_LAPS
        WHERE session_key IN ({session_filter})
    """).to_pandas()["N"].iloc[0]
    fastest_lap = session.sql(f"""
        SELECT MIN(lap_duration_s) AS fl FROM APEXML_DB.PROD.FCT_LAPS
        WHERE session_key IN ({session_filter})
    """).to_pandas()["FL"].iloc[0]
    pit_count = session.sql(f"""
        SELECT COUNT(*) AS n FROM APEXML_DB.PROD.FCT_PIT_STOPS
        WHERE session_key IN ({session_filter})
    """).to_pandas()["N"].iloc[0]
    col1.metric("Total Laps", laps_count)
    col2.metric("Fastest Lap", f"{fastest_lap:.3f}s" if fastest_lap else "N/A")
    col3.metric("Pit Stops", pit_count)
    col4.metric("Drivers", len(selected_drivers))

    st.divider()

    # ── Starting Grid ──────────────────────────────────────────────────────────
    st.subheader("Starting Grid")
    grid_raw = session.sql(f"""
        SELECT sg.driver_number, sg.driver_name, sg.team_name, sg.grid_position,
               s.meeting_name || ' — ' || s.session_name AS race_label
        FROM APEXML_DB.PROD.FCT_RACE_POSITIONS rp
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON rp.session_key = s.session_key
        JOIN APEXML_DB.RAW.STARTING_GRID sg
            ON sg.meeting_key = s.meeting_key
            AND sg.driver_number = rp.driver_number
        WHERE rp.session_key IN ({session_filter})
          AND rp.driver_number IN ({driver_filter})
        QUALIFY ROW_NUMBER() OVER (PARTITION BY s.session_key, sg.driver_number ORDER BY sg.grid_position) = 1
        ORDER BY race_label, sg.grid_position
    """).to_pandas()
    if not grid_raw.empty:
        st.dataframe(grid_raw, use_container_width=True)
    else:
        st.info("No starting grid data for selected sessions.")

    st.divider()

    # ── Grid vs Finish ─────────────────────────────────────────────────────────
    st.subheader("Grid vs Finish Position")
    result_data = session.sql(f"""
        SELECT r.driver_name, r.grid_position, r.finish_position, r.positions_gained,
               r.classified_position, r.points,
               s.meeting_name || ' — ' || s.session_name AS race_label
        FROM APEXML_DB.PROD.FCT_SESSION_RESULTS r
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON r.session_key = s.session_key
        WHERE r.session_key IN ({session_filter})
          AND r.driver_number IN ({driver_filter})
        ORDER BY s.session_start_at, r.finish_position
    """).to_pandas()

    if not result_data.empty:
        col1, col2 = st.columns(2)
        with col1:
            chart = alt.Chart(result_data).mark_point(size=100, filled=True).encode(
                x=alt.X("GRID_POSITION:Q", title="Grid Position"),
                y=alt.Y("FINISH_POSITION:Q", title="Finish Position", scale=alt.Scale(reverse=True)),
                color=alt.Color("DRIVER_NAME:N", scale=color_scale),
                shape=alt.Shape("RACE_LABEL:N"),
                tooltip=["DRIVER_NAME", "RACE_LABEL", "GRID_POSITION", "FINISH_POSITION", "POSITIONS_GAINED", "POINTS"],
            ).properties(height=350)
            line = alt.Chart(
                {"values": [{"x": 1, "y": 1}, {"x": 20, "y": 20}]}
            ).mark_line(strokeDash=[4, 4], color="gray").encode(x="x:Q", y="y:Q")
            st.altair_chart(chart + line, use_container_width=True)
        with col2:
            st.dataframe(
                result_data[["RACE_LABEL", "DRIVER_NAME", "GRID_POSITION", "FINISH_POSITION", "POSITIONS_GAINED", "CLASSIFIED_POSITION", "POINTS"]],
                use_container_width=True
            )
    else:
        st.info("No result data for selected sessions/drivers.")

    st.divider()

    # ── Race Positions ─────────────────────────────────────────────────────────
    st.subheader("Race Positions")
    pos_data = session.sql(f"""
        SELECT rp.recorded_at AS ts, rp.position, rp.driver_number,
               d.full_name,
               s.meeting_name || ' — ' || s.session_name AS race_label
        FROM APEXML_DB.PROD.FCT_RACE_POSITIONS rp
        JOIN APEXML_DB.PROD.DIM_DRIVERS d ON rp.driver_number = d.driver_number AND rp.session_key = d.session_key
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON rp.session_key = s.session_key
        WHERE rp.session_key IN ({session_filter})
          AND rp.driver_number IN ({driver_filter})
        ORDER BY ts
    """).to_pandas()
    if not pos_data.empty:
        chart = alt.Chart(pos_data).mark_line().encode(
            x=alt.X("TS:T", title="Time"),
            y=alt.Y("POSITION:Q", title="Position", scale=alt.Scale(reverse=True)),
            color=alt.Color("FULL_NAME:N", scale=color_scale),
            strokeDash=alt.StrokeDash("RACE_LABEL:N"),
            tooltip=["FULL_NAME", "RACE_LABEL", "POSITION", "TS"],
        ).properties(height=350)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No position data available.")

    st.divider()

    # ── Lap Times ─────────────────────────────────────────────────────────────
    st.subheader("Lap Times")
    lap_data = session.sql(f"""
        SELECT l.lap_number, l.lap_duration_s, l.driver_name, l.tyre_compound,
               s.meeting_name || ' — ' || s.session_name AS race_label
        FROM APEXML_DB.PROD.FCT_LAPS l
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON l.session_key = s.session_key
        WHERE l.session_key IN ({session_filter})
          AND l.driver_number IN ({driver_filter})
        ORDER BY race_label, l.lap_number
    """).to_pandas()
    if not lap_data.empty:
        chart = alt.Chart(lap_data).mark_line(opacity=0.8).encode(
            x=alt.X("LAP_NUMBER:Q", title="Lap"),
            y=alt.Y("LAP_DURATION_S:Q", title="Lap Time (s)", scale=alt.Scale(zero=False)),
            color=alt.Color("DRIVER_NAME:N", scale=color_scale),
            strokeDash=alt.StrokeDash("RACE_LABEL:N"),
            tooltip=["DRIVER_NAME", "RACE_LABEL", "LAP_NUMBER", "LAP_DURATION_S", "TYRE_COMPOUND"],
        ).properties(height=350)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No lap data for selected sessions/drivers.")

    st.divider()

    # ── Tyre Strategy ─────────────────────────────────────────────────────────
    st.subheader("Tyre Strategy")
    stint_data = session.sql(f"""
        SELECT st.driver_name, st.tyre_compound, st.lap_start, st.lap_end,
               st.stint_length, st.avg_lap_time_s, st.fastest_lap_s,
               s.meeting_name || ' — ' || s.session_name AS race_label
        FROM APEXML_DB.PROD.FCT_STINTS st
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON st.session_key = s.session_key
        WHERE st.session_key IN ({session_filter})
          AND st.driver_number IN ({driver_filter})
        ORDER BY race_label, st.driver_name, st.lap_start
    """).to_pandas()
    if not stint_data.empty:
        tyre_domain = list(TYRE_COLORS.keys())
        tyre_range = list(TYRE_COLORS.values())
        for race_label in stint_data["RACE_LABEL"].unique():
            st.markdown(f"**{race_label}**")
            race_stints = stint_data[stint_data["RACE_LABEL"] == race_label]
            chart = alt.Chart(race_stints).mark_bar(height=18).encode(
                x=alt.X("LAP_START:Q", title="Lap"),
                x2="LAP_END:Q",
                y=alt.Y("DRIVER_NAME:N", title="Driver", sort="-x"),
                color=alt.Color(
                    "TYRE_COMPOUND:N",
                    scale=alt.Scale(domain=tyre_domain, range=tyre_range),
                    legend=alt.Legend(title="Compound")
                ),
                tooltip=["DRIVER_NAME", "TYRE_COMPOUND", "LAP_START", "LAP_END", "STINT_LENGTH", "AVG_LAP_TIME_S", "FASTEST_LAP_S"],
            ).properties(height=max(200, len(race_stints["DRIVER_NAME"].unique()) * 28))
            st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No stint data for selected sessions/drivers.")

    st.divider()

    # ── Pit Stops ─────────────────────────────────────────────────────────────
    st.subheader("Pit Stops")
    pit_data = session.sql(f"""
        SELECT p.driver_name, p.lap_number, p.pit_duration_s,
               p.tyre_compound_out, p.tyre_compound_in,
               s.meeting_name || ' — ' || s.session_name AS race_label
        FROM APEXML_DB.PROD.FCT_PIT_STOPS p
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON p.session_key = s.session_key
        WHERE p.session_key IN ({session_filter})
          AND p.driver_number IN ({driver_filter})
        ORDER BY race_label, p.lap_number
    """).to_pandas()
    if not pit_data.empty:
        st.dataframe(pit_data, use_container_width=True)
    else:
        st.info("No pit stop data for selected sessions/drivers.")

    st.divider()

    # ── Car Telemetry ─────────────────────────────────────────────────────────
    st.subheader("Car Telemetry")
    st.caption("Sampled — showing average per lap per driver to keep it readable.")
    telemetry_data = session.sql(f"""
        SELECT
            d.full_name AS driver_name,
            l.lap_number,
            AVG(c.speed) AS avg_speed,
            AVG(c.throttle) AS avg_throttle,
            AVG(c.brake) AS brake_pct,
            AVG(c.rpm) AS avg_rpm,
            s.meeting_name || ' — ' || s.session_name AS race_label
        FROM APEXML_DB.PROD.FCT_CAR_DATA c
        JOIN APEXML_DB.PROD.FCT_LAPS l
            ON c.session_key = l.session_key
            AND c.driver_number = l.driver_number
            AND c.recorded_at BETWEEN l.lap_start_at AND l.lap_end_at
        JOIN APEXML_DB.PROD.DIM_DRIVERS d ON c.driver_number = d.driver_number AND c.session_key = d.session_key
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON c.session_key = s.session_key
        WHERE c.session_key IN ({session_filter})
          AND c.driver_number IN ({driver_filter})
        GROUP BY d.full_name, l.lap_number, race_label
        ORDER BY race_label, l.lap_number
    """).to_pandas()
    if not telemetry_data.empty:
        for metric, title in [("AVG_SPEED", "Avg Speed (km/h)"), ("AVG_THROTTLE", "Avg Throttle (%)"), ("BRAKE_PCT", "Brake Usage (%)"), ("AVG_RPM", "Avg RPM")]:
            chart = alt.Chart(telemetry_data).mark_line(opacity=0.8).encode(
                x=alt.X("LAP_NUMBER:Q", title="Lap"),
                y=alt.Y(f"{metric}:Q", title=title, scale=alt.Scale(zero=False)),
                color=alt.Color("DRIVER_NAME:N", scale=color_scale),
                strokeDash=alt.StrokeDash("RACE_LABEL:N"),
                tooltip=["DRIVER_NAME", "RACE_LABEL", "LAP_NUMBER", f"{metric}"],
            ).properties(height=250, title=title)
            st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No telemetry data for selected sessions/drivers.")

    st.divider()

    # ── Intervals ─────────────────────────────────────────────────────────────
    st.subheader("Gap to Leader")
    interval_data = session.sql(f"""
        SELECT i.gap_to_leader, i.interval_to_car_ahead, i.recorded_at,
               d.full_name AS driver_name,
               s.meeting_name || ' — ' || s.session_name AS race_label
        FROM APEXML_DB.PROD.FCT_INTERVALS i
        JOIN APEXML_DB.PROD.DIM_DRIVERS d ON i.driver_number = d.driver_number AND i.session_key = d.session_key
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON i.session_key = s.session_key
        WHERE i.session_key IN ({session_filter})
          AND i.driver_number IN ({driver_filter})
        ORDER BY i.recorded_at
    """).to_pandas()
    if not interval_data.empty:
        chart = alt.Chart(interval_data).mark_line(opacity=0.8).encode(
            x=alt.X("RECORDED_AT:T", title="Time"),
            y=alt.Y("GAP_TO_LEADER:Q", title="Gap to Leader (s)", scale=alt.Scale(zero=False)),
            color=alt.Color("DRIVER_NAME:N", scale=color_scale),
            strokeDash=alt.StrokeDash("RACE_LABEL:N"),
            tooltip=["DRIVER_NAME", "RACE_LABEL", "GAP_TO_LEADER", "INTERVAL_TO_CAR_AHEAD", "RECORDED_AT"],
        ).properties(height=350)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No interval data for selected sessions/drivers.")
