import streamlit as st
import pandas as pd
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.colors import TEAM_COLORS

TYRE_COLORS = {
    "SOFT":         "#E8002D",
    "MEDIUM":       "#FFF200",
    "HARD":         "#FFFFFF",
    "INTERMEDIATE": "#39B54A",
    "WET":          "#0067FF",
}

TYRE_LABELS = {
    "SOFT": "S", "MEDIUM": "M", "HARD": "H",
    "INTERMEDIATE": "I", "WET": "W",
}

CSS = """
<style>
.block-container { padding-top: 1rem !important; }
.f1-metric-row { display: flex; gap: 12px; margin-bottom: 16px; flex-wrap: wrap; }
.f1-metric {
    background: #1a1a1a; border: 1px solid #2a2a2a;
    border-left: 3px solid #e10600; padding: 10px 18px;
    min-width: 130px; flex: 1;
}
.f1-metric .label { font-size: 10px; color: #888; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 4px; }
.f1-metric .value { font-size: 22px; font-weight: 700; color: #fff; font-family: monospace; }
.f1-section {
    font-size: 11px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 2px; color: #888; margin: 20px 0 8px 0;
    border-bottom: 1px solid #2a2a2a; padding-bottom: 4px;
}
.stint-row { display: flex; align-items: center; gap: 6px; margin: 4px 0; font-size: 12px; font-family: monospace; }
.stint-driver { color: #fff; font-weight: 700; width: 80px; flex-shrink: 0; font-size: 13px; }
.stint-seg {
    display: inline-block; height: 20px; line-height: 20px;
    font-size: 10px; font-weight: 900; color: #111; text-align: center;
    border-radius: 2px; padding: 0 6px; white-space: nowrap; min-width: 20px;
}
</style>
"""


def fmt_laptime(seconds):
    if seconds is None or (isinstance(seconds, float) and pd.isna(seconds)):
        return "—"
    m = int(seconds // 60)
    s = seconds % 60
    return f"{m}:{s:06.3f}"


def safe_compound(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "UNKNOWN"
    return str(val).upper()


def tyre_dot(compound):
    c = safe_compound(compound)
    color = TYRE_COLORS.get(c, "#555")
    label = TYRE_LABELS.get(c, "?")
    return f"{label}"


def render(session):
    st.markdown(CSS, unsafe_allow_html=True)

    # ── Filters ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown('<div class="f1-section">Filters</div>', unsafe_allow_html=True)

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
        selected_labels = st.multiselect("Sessions", session_labels, default=session_labels[-1:] if session_labels else [])

        if not selected_labels:
            st.warning("Select at least one session.")
            return

        selected_keys = sessions_df[sessions_df["LABEL"].isin(selected_labels)]["SESSION_KEY"].tolist()
        session_filter = ", ".join(str(k) for k in selected_keys)

        drivers_df = session.sql(f"""
            SELECT DISTINCT driver_number, full_name, acronym, team_name
            FROM APEXML_DB.PROD.DIM_DRIVERS
            WHERE session_key IN ({session_filter})
            ORDER BY full_name
        """).to_pandas()

        driver_options = drivers_df["FULL_NAME"].drop_duplicates().tolist()
        selected_drivers = st.multiselect("Drivers", driver_options, default=driver_options)

        if not selected_drivers:
            st.warning("Select at least one driver.")
            return

        selected_driver_numbers = drivers_df[
            drivers_df["FULL_NAME"].isin(selected_drivers)
        ]["DRIVER_NUMBER"].drop_duplicates().tolist()
        driver_filter = ", ".join(str(d) for d in selected_driver_numbers)

    # ── Summary metrics ───────────────────────────────────────────────────────
    metrics = session.sql(f"""
        SELECT COUNT(*) AS total_laps, MIN(lap_duration_s) AS fastest_lap,
               MAX(lap_number) AS max_lap, COUNT(DISTINCT driver_number) AS driver_count
        FROM APEXML_DB.PROD.FCT_LAPS
        WHERE session_key IN ({session_filter})
    """).to_pandas().iloc[0]

    pit_count = session.sql(f"""
        SELECT COUNT(*) AS n FROM APEXML_DB.PROD.FCT_PIT_STOPS
        WHERE session_key IN ({session_filter})
    """).to_pandas()["N"].iloc[0]

    fl = metrics["FASTEST_LAP"]
    max_lap = int(metrics["MAX_LAP"]) if pd.notna(metrics["MAX_LAP"]) else "—"
    st.markdown(f"""
    <div class="f1-metric-row">
        <div class="f1-metric"><div class="label">Total Laps</div><div class="value">{int(metrics['TOTAL_LAPS'])}</div></div>
        <div class="f1-metric"><div class="label">Fastest Lap</div><div class="value">{fmt_laptime(fl)}</div></div>
        <div class="f1-metric"><div class="label">Pit Stops</div><div class="value">{int(pit_count)}</div></div>
        <div class="f1-metric"><div class="label">Race Laps</div><div class="value">{max_lap}</div></div>
        <div class="f1-metric"><div class="label">Drivers</div><div class="value">{int(metrics['DRIVER_COUNT'])}</div></div>
    </div>
    """, unsafe_allow_html=True)

    # ── Race Standings ─────────────────────────────────────────────────────────
    st.markdown('<div class="f1-section">Race Standings</div>', unsafe_allow_html=True)

    results = session.sql(f"""
        SELECT r.driver_name, r.driver_acronym, r.team_name,
               r.finish_position, r.grid_position, r.positions_gained,
               r.points, r.classified_position,
               s.meeting_name || ' — ' || s.session_name AS race_label
        FROM APEXML_DB.PROD.FCT_SESSION_RESULTS r
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON r.session_key = s.session_key
        WHERE r.session_key IN ({session_filter})
          AND r.driver_number IN ({driver_filter})
        ORDER BY race_label, r.finish_position
    """).to_pandas()

    best_laps = session.sql(f"""
        SELECT driver_name, MIN(lap_duration_s) AS best_lap, tyre_compound
        FROM APEXML_DB.PROD.FCT_LAPS
        WHERE session_key IN ({session_filter})
          AND driver_number IN ({driver_filter})
        GROUP BY driver_name, tyre_compound
        QUALIFY ROW_NUMBER() OVER (PARTITION BY driver_name ORDER BY MIN(lap_duration_s)) = 1
    """).to_pandas()

    if not results.empty:
        for race_label in results["RACE_LABEL"].unique():
            race = results[results["RACE_LABEL"] == race_label].copy()
            race = race.merge(best_laps[["DRIVER_NAME", "BEST_LAP", "TYRE_COMPOUND"]], on="DRIVER_NAME", how="left")

            leader_best = race["BEST_LAP"].min()

            display = pd.DataFrame()
            display["POS"] = race["FINISH_POSITION"].apply(lambda x: int(x) if pd.notna(x) else "—")
            display["DRIVER"] = race["DRIVER_ACRONYM"]
            display["NAME"] = race["DRIVER_NAME"]
            display["TEAM"] = race["TEAM_NAME"]
            display["TYRE"] = race["TYRE_COMPOUND"].apply(lambda x: TYRE_LABELS.get(safe_compound(x), "?"))
            display["BEST LAP"] = race["BEST_LAP"].apply(fmt_laptime)
            display["GAP"] = race.apply(
                lambda r: "LEADER" if pd.notna(r["BEST_LAP"]) and r["BEST_LAP"] == leader_best
                else (f"+{r['BEST_LAP'] - leader_best:.3f}s" if pd.notna(r["BEST_LAP"]) else "—"), axis=1
            )
            display["GRID"] = race["GRID_POSITION"].apply(lambda x: f"P{int(x)}" if pd.notna(x) else "—")
            display["+/-"] = race["POSITIONS_GAINED"].apply(
                lambda x: (f"+{int(x)}" if x > 0 else str(int(x))) if pd.notna(x) and x != 0 else "—"
            )
            display["PTS"] = race["POINTS"].apply(lambda x: int(x) if pd.notna(x) and x > 0 else "—")

            st.caption(race_label)

            def style_standings(df):
                styles = pd.DataFrame("", index=df.index, columns=df.columns)
                for i, row in df.iterrows():
                    team_color = TEAM_COLORS.get(results.loc[results["DRIVER_ACRONYM"] == row["DRIVER"], "TEAM_NAME"].values[0] if len(results.loc[results["DRIVER_ACRONYM"] == row["DRIVER"]]) > 0 else "", "#888")
                    styles.at[i, "DRIVER"] = f"color: {team_color}; font-weight: 700; font-family: monospace;"
                    styles.at[i, "TEAM"] = f"color: {team_color}; font-size: 11px;"
                    styles.at[i, "BEST LAP"] = "color: #39FF14; font-weight: 600; font-family: monospace;"
                    styles.at[i, "GAP"] = "color: #e10600; font-family: monospace;" if row["GAP"] != "LEADER" else "color: #39FF14; font-weight: 700;"
                    styles.at[i, "POS"] = "font-weight: 700; font-size: 14px; color: #fff;"
                    if str(row.get("+/-", "—")).startswith("+"):
                        styles.at[i, "+/-"] = "color: #39FF14;"
                    elif str(row.get("+/-", "—")).startswith("-"):
                        styles.at[i, "+/-"] = "color: #e10600;"
                    tyre_color = TYRE_COLORS.get(
                        safe_compound(race.loc[race["DRIVER_ACRONYM"] == row["DRIVER"], "TYRE_COMPOUND"].values[0] if len(race.loc[race["DRIVER_ACRONYM"] == row["DRIVER"]]) > 0 else ""),
                        "#555"
                    )
                    styles.at[i, "TYRE"] = f"color: {tyre_color}; font-weight: 900; text-align: center;"
                return styles

            st.dataframe(
                display.style.apply(style_standings, axis=None),
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.info("No result data for selected sessions/drivers.")

    # ── Lap Times ─────────────────────────────────────────────────────────────
    st.markdown('<div class="f1-section">Lap Times</div>', unsafe_allow_html=True)

    lap_data = session.sql(f"""
        SELECT l.lap_number, l.lap_duration_s, l.driver_name, l.driver_acronym,
               l.tyre_compound, l.is_pit_out_lap,
               s.meeting_name || ' — ' || s.session_name AS race_label
        FROM APEXML_DB.PROD.FCT_LAPS l
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON l.session_key = s.session_key
        WHERE l.session_key IN ({session_filter})
          AND l.driver_number IN ({driver_filter})
        ORDER BY race_label, l.lap_number, l.driver_name
    """).to_pandas()

    if not lap_data.empty:
        for race_label in lap_data["RACE_LABEL"].unique():
            race_laps = lap_data[lap_data["RACE_LABEL"] == race_label].copy()
            st.caption(race_label)

            summary = race_laps.groupby(["DRIVER_ACRONYM", "DRIVER_NAME"]).agg(
                BEST=("LAP_DURATION_S", "min"),
                AVG=("LAP_DURATION_S", "mean"),
                WORST=("LAP_DURATION_S", "max"),
                LAPS=("LAP_NUMBER", "count"),
            ).reset_index().sort_values("BEST")

            overall_best = summary["BEST"].min()

            display = pd.DataFrame()
            display["ACR"] = summary["DRIVER_ACRONYM"]
            display["DRIVER"] = summary["DRIVER_NAME"]
            display["BEST LAP"] = summary["BEST"].apply(fmt_laptime)
            display["GAP"] = summary["BEST"].apply(
                lambda x: "FASTEST" if x == overall_best else f"+{x - overall_best:.3f}s"
            )
            display["AVG LAP"] = summary["AVG"].apply(fmt_laptime)
            display["WORST LAP"] = summary["WORST"].apply(fmt_laptime)
            display["LAPS"] = summary["LAPS"].astype(int)

            def style_laps(df):
                styles = pd.DataFrame("", index=df.index, columns=df.columns)
                for i, row in df.iterrows():
                    styles.at[i, "ACR"] = "font-weight: 700; color: #fff; font-family: monospace;"
                    styles.at[i, "BEST LAP"] = "color: #39FF14; font-weight: 600; font-family: monospace;"
                    styles.at[i, "GAP"] = "color: #39FF14; font-weight: 700;" if row["GAP"] == "FASTEST" else "color: #e10600; font-family: monospace;"
                    styles.at[i, "WORST LAP"] = "color: #e10600; font-family: monospace;"
                    styles.at[i, "AVG LAP"] = "font-family: monospace;"
                return styles

            st.dataframe(
                display.style.apply(style_laps, axis=None),
                use_container_width=True,
                hide_index=True,
            )

            with st.expander(f"Lap-by-lap — {race_label}"):
                pivot = race_laps.pivot_table(
                    index="LAP_NUMBER", columns="DRIVER_ACRONYM", values="LAP_DURATION_S"
                ).round(3)
                st.dataframe(
                    pivot.style.format(lambda x: fmt_laptime(x) if pd.notna(x) else "—"),
                    use_container_width=True,
                )
    else:
        st.info("No lap data for selected sessions/drivers.")

    # ── Grid vs Finish ─────────────────────────────────────────────────────────
    st.markdown('<div class="f1-section">Grid → Finish</div>', unsafe_allow_html=True)

    grid_finish = session.sql(f"""
        SELECT r.driver_acronym, r.driver_name, r.team_name,
               r.grid_position, r.finish_position, r.positions_gained, r.points,
               s.meeting_name || ' — ' || s.session_name AS race_label
        FROM APEXML_DB.PROD.FCT_SESSION_RESULTS r
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON r.session_key = s.session_key
        WHERE r.session_key IN ({session_filter})
          AND r.driver_number IN ({driver_filter})
        ORDER BY race_label, r.finish_position
    """).to_pandas()

    if not grid_finish.empty:
        for race_label in grid_finish["RACE_LABEL"].unique():
            race = grid_finish[grid_finish["RACE_LABEL"] == race_label].copy()
            st.caption(race_label)

            display = pd.DataFrame()
            display["FINISH"] = race["FINISH_POSITION"].apply(lambda x: f"P{int(x)}" if pd.notna(x) else "—")
            display["ACR"]    = race["DRIVER_ACRONYM"]
            display["DRIVER"] = race["DRIVER_NAME"]
            display["TEAM"]   = race["TEAM_NAME"]
            display["GRID"]   = race["GRID_POSITION"].apply(lambda x: f"P{int(x)}" if pd.notna(x) else "—")
            display["+/-"]    = race["POSITIONS_GAINED"].apply(
                lambda x: (f"+{int(x)}" if x > 0 else str(int(x))) if pd.notna(x) and x != 0 else "—"
            )
            display["PTS"]    = race["POINTS"].apply(lambda x: int(x) if pd.notna(x) and x > 0 else "—")

            def style_grid_finish(df):
                styles = pd.DataFrame("", index=df.index, columns=df.columns)
                for i, row in df.iterrows():
                    team_color = TEAM_COLORS.get(race.iloc[i]["TEAM_NAME"], "#888")
                    styles.at[i, "ACR"]    = f"color: {team_color}; font-weight: 700; font-family: monospace;"
                    styles.at[i, "TEAM"]   = f"color: {team_color}; font-size: 11px;"
                    styles.at[i, "FINISH"] = "font-weight: 700; color: #fff; font-family: monospace;"
                    styles.at[i, "GRID"]   = "color: #888; font-family: monospace;"
                    pg = row["+/-"]
                    if str(pg).startswith("+"):
                        styles.at[i, "+/-"] = "color: #39FF14; font-weight: 700;"
                    elif str(pg).startswith("-"):
                        styles.at[i, "+/-"] = "color: #e10600; font-weight: 700;"
                    styles.at[i, "PTS"] = "color: #FFF200; font-weight: 700;"
                return styles

            st.dataframe(
                display.style.apply(style_grid_finish, axis=None),
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.info("No results data for selected sessions.")

    # ── Race Positions Grid ────────────────────────────────────────────────────
    st.markdown('<div class="f1-section">Race Positions — Lap by Lap</div>', unsafe_allow_html=True)

    pos_data = session.sql(f"""
        WITH lb AS (
            SELECT session_key, driver_number, lap_number, lap_start_at,
                   LEAD(lap_start_at) OVER (
                       PARTITION BY session_key, driver_number ORDER BY lap_number
                   ) AS lap_end_at
            FROM APEXML_DB.PROD.FCT_LAPS
            WHERE session_key IN ({session_filter})
        ),
        pos_per_lap AS (
            SELECT
                lb.session_key, lb.driver_number, lb.lap_number,
                rp.position, rp.driver_acronym, rp.driver_name, rp.team_name
            FROM lb
            LEFT JOIN APEXML_DB.PROD.FCT_RACE_POSITIONS rp
                ON  rp.session_key   = lb.session_key
                AND rp.driver_number = lb.driver_number
                AND rp.recorded_at  >= lb.lap_start_at
                AND rp.recorded_at   < COALESCE(lb.lap_end_at, DATEADD('hour', 1, lb.lap_start_at))
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY lb.session_key, lb.driver_number, lb.lap_number
                ORDER BY rp.recorded_at DESC
            ) = 1
        )
        SELECT
            p.position, p.driver_acronym, p.driver_name, p.team_name, p.lap_number,
            s.meeting_name || ' — ' || s.session_name AS race_label
        FROM pos_per_lap p
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON p.session_key = s.session_key
        ORDER BY race_label, p.lap_number, p.position
    """).to_pandas()

    if not pos_data.empty:
        driver_team_map = pos_data.drop_duplicates("DRIVER_ACRONYM").set_index("DRIVER_ACRONYM")["TEAM_NAME"].to_dict()

        for race_label in pos_data["RACE_LABEL"].unique():
            st.caption(race_label)
            race_pos = pos_data[pos_data["RACE_LABEL"] == race_label]

            pivot = race_pos.pivot_table(
                index="LAP_NUMBER",
                columns="POSITION",
                values="DRIVER_ACRONYM",
                aggfunc="first"
            )
            pivot.columns = [f"P{int(c)}" for c in sorted(pivot.columns)]
            pivot.index.name = "LAP"

            def color_cell(val):
                if not val or (isinstance(val, float) and pd.isna(val)):
                    return "background-color: #111; color: #444;"
                team = driver_team_map.get(val, "")
                bg = TEAM_COLORS.get(team, "#2a2a2a")
                return f"background-color: {bg}22; color: #fff; font-weight: 700; font-family: monospace; text-align: center; border: 1px solid {bg}55;"

            st.dataframe(
                pivot.style.map(color_cell).format(lambda x: x if isinstance(x, str) else "—"),
                use_container_width=True,
                height=min(800, len(pivot) * 35 + 40),
            )
    else:
        st.info("No race position data for selected sessions.")

    # ── Tyre Strategy ─────────────────────────────────────────────────────────
    st.markdown('<div class="f1-section">Tyre Strategy</div>', unsafe_allow_html=True)

    stint_data = session.sql(f"""
        SELECT st.driver_name, st.driver_acronym, st.tyre_compound,
               st.lap_start, st.lap_end, st.stint_length,
               st.avg_lap_time_s, st.fastest_lap_s,
               s.meeting_name || ' — ' || s.session_name AS race_label
        FROM APEXML_DB.PROD.FCT_STINTS st
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON st.session_key = s.session_key
        WHERE st.session_key IN ({session_filter})
          AND st.driver_number IN ({driver_filter})
        ORDER BY race_label, st.driver_name, st.lap_start
    """).to_pandas()

    if not stint_data.empty:
        for race_label in stint_data["RACE_LABEL"].unique():
            race_stints = stint_data[stint_data["RACE_LABEL"] == race_label]
            st.caption(race_label)
            max_lap = int(race_stints["LAP_END"].max())

            rows_html = ""
            for driver in race_stints["DRIVER_NAME"].unique():
                driver_stints = race_stints[race_stints["DRIVER_NAME"] == driver].sort_values("LAP_START")
                acronym = driver_stints["DRIVER_ACRONYM"].iloc[0]
                segs = ""
                for _, s in driver_stints.iterrows():
                    compound = safe_compound(s["TYRE_COMPOUND"])
                    color = TYRE_COLORS.get(compound, "#555")
                    label = TYRE_LABELS.get(compound, "?")
                    lap_start = int(s["LAP_START"])
                    lap_end = int(s["LAP_END"])
                    stint_len = int(s["STINT_LENGTH"])
                    avg = fmt_laptime(s["AVG_LAP_TIME_S"]) if pd.notna(s.get("AVG_LAP_TIME_S")) else "—"
                    pct = max(20, round((stint_len / max_lap) * 600))
                    txt_color = "#111" if compound != "WET" else "#fff"
                    segs += f'<span class="stint-seg" style="background:{color};width:{pct}px;color:{txt_color};" title="L{lap_start}–{lap_end} | {stint_len} laps | avg {avg}">{label}</span>'
                rows_html += f'<div class="stint-row"><span class="stint-driver">{acronym}</span>{segs}</div>'

            st.markdown(rows_html, unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)
    else:
        st.info("No stint data for selected sessions/drivers.")

    # ── Pit Stops ─────────────────────────────────────────────────────────────
    st.markdown('<div class="f1-section">Pit Stops</div>', unsafe_allow_html=True)

    pit_data = session.sql(f"""
        SELECT p.driver_name, p.driver_acronym, p.lap_number,
               p.pit_duration_s, p.tyre_compound_out, p.tyre_compound_in,
               s.meeting_name || ' — ' || s.session_name AS race_label
        FROM APEXML_DB.PROD.FCT_PIT_STOPS p
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON p.session_key = s.session_key
        WHERE p.session_key IN ({session_filter})
          AND p.driver_number IN ({driver_filter})
        ORDER BY race_label, p.lap_number, p.driver_name
    """).to_pandas()

    if not pit_data.empty:
        for race_label in pit_data["RACE_LABEL"].unique():
            race_pits = pit_data[pit_data["RACE_LABEL"] == race_label].copy()
            st.caption(race_label)

            fastest = race_pits["PIT_DURATION_S"].min()

            display = pd.DataFrame()
            display["ACR"] = race_pits["DRIVER_ACRONYM"]
            display["DRIVER"] = race_pits["DRIVER_NAME"]
            display["LAP"] = race_pits["LAP_NUMBER"].apply(lambda x: f"L{int(x)}" if pd.notna(x) else "—")
            display["STOP TIME"] = race_pits["PIT_DURATION_S"].apply(
                lambda x: f"{x:.2f}s" if pd.notna(x) else "—"
            )
            display["OUT"] = race_pits["TYRE_COMPOUND_OUT"].apply(
                lambda x: TYRE_LABELS.get(safe_compound(x), "?") if pd.notna(x) else "—"
            )
            display["IN"] = race_pits["TYRE_COMPOUND_IN"].apply(
                lambda x: TYRE_LABELS.get(safe_compound(x), "?") if pd.notna(x) else "—"
            )

            def style_pits(df):
                styles = pd.DataFrame("", index=df.index, columns=df.columns)
                for i, row in df.iterrows():
                    styles.at[i, "ACR"] = "font-weight: 700; color: #fff; font-family: monospace;"
                    pit_dur = race_pits.iloc[i]["PIT_DURATION_S"]
                    if pd.notna(pit_dur):
                        if pit_dur == fastest:
                            styles.at[i, "STOP TIME"] = "color: #39FF14; font-weight: 700; font-family: monospace;"
                        elif pit_dur > fastest * 1.5:
                            styles.at[i, "STOP TIME"] = "color: #e10600; font-family: monospace;"
                        else:
                            styles.at[i, "STOP TIME"] = "font-family: monospace;"
                return styles

            st.dataframe(
                display.style.apply(style_pits, axis=None),
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.info("No pit stop data for selected sessions/drivers.")

    # ── Car Telemetry ─────────────────────────────────────────────────────────
    st.markdown('<div class="f1-section">Car Telemetry (avg per lap)</div>', unsafe_allow_html=True)

    telemetry_data = session.sql(f"""
        WITH lb AS (
            SELECT session_key, driver_number, lap_number, lap_start_at,
                   LEAD(lap_start_at) OVER (
                       PARTITION BY session_key, driver_number ORDER BY lap_number
                   ) AS lap_end_at
            FROM APEXML_DB.PROD.FCT_LAPS
            WHERE session_key IN ({session_filter})
              AND driver_number IN ({driver_filter})
        )
        SELECT
            c.driver_name, c.driver_acronym, lb.lap_number,
            ROUND(AVG(c.speed_kmh), 1)                 AS speed,
            ROUND(AVG(c.throttle_pct), 1)              AS throttle,
            ROUND(AVG(c.is_braking::integer) * 100, 1) AS brake,
            ROUND(AVG(c.gear), 2)                      AS n_gear,
            ROUND(AVG(c.rpm), 0)                       AS rpm,
            ROUND(AVG(c.drs_status), 2)                AS drs,
            s.meeting_name || ' — ' || s.session_name  AS race_label
        FROM APEXML_DB.PROD.FCT_CAR_DATA c
        JOIN lb ON c.session_key = lb.session_key AND c.driver_number = lb.driver_number
            AND c.recorded_at >= lb.lap_start_at
            AND c.recorded_at < COALESCE(lb.lap_end_at, DATEADD('hour', 1, lb.lap_start_at))
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON c.session_key = s.session_key
        GROUP BY c.driver_name, c.driver_acronym, lb.lap_number, race_label
        ORDER BY race_label, lb.lap_number, c.driver_acronym
    """).to_pandas()

    if not telemetry_data.empty:
        for race_label in telemetry_data["RACE_LABEL"].unique():
            race_tel = telemetry_data[telemetry_data["RACE_LABEL"] == race_label]
            st.caption(race_label)

            metric_tabs = st.tabs(["Speed", "Throttle", "Brake", "Gear", "RPM", "DRS"])
            for tab, (col, unit) in zip(metric_tabs, [
                ("SPEED", "km/h"), ("THROTTLE", "0–100"), ("BRAKE", "% braking"),
                ("N_GEAR", "avg gear"), ("RPM", "rpm"), ("DRS", "status"),
            ]):
                with tab:
                    pivot = race_tel.pivot_table(
                        index="LAP_NUMBER", columns="DRIVER_ACRONYM", values=col
                    ).round(2)
                    st.dataframe(
                        pivot.style.format(lambda x: f"{x} {unit}" if pd.notna(x) else "—"),
                        use_container_width=True,
                    )
    else:
        st.info("No telemetry data for selected sessions/drivers.")

    # ── Gap to Leader ──────────────────────────────────────────────────────────
    st.markdown('<div class="f1-section">Gap to Leader</div>', unsafe_allow_html=True)

    interval_data = session.sql(f"""
        WITH lb AS (
            SELECT session_key, driver_number, lap_number, lap_start_at,
                   LEAD(lap_start_at) OVER (
                       PARTITION BY session_key, driver_number ORDER BY lap_number
                   ) AS lap_end_at
            FROM APEXML_DB.PROD.FCT_LAPS
            WHERE session_key IN ({session_filter})
              AND driver_number IN ({driver_filter})
        )
        SELECT i.gap_to_leader_s, i.driver_acronym, lb.lap_number,
               s.meeting_name || ' — ' || s.session_name AS race_label
        FROM APEXML_DB.PROD.FCT_INTERVALS i
        JOIN lb ON i.session_key = lb.session_key AND i.driver_number = lb.driver_number
            AND i.recorded_at >= lb.lap_start_at
            AND i.recorded_at < COALESCE(lb.lap_end_at, DATEADD('hour', 1, lb.lap_start_at))
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON i.session_key = s.session_key
        WHERE i.session_key IN ({session_filter})
          AND i.driver_number IN ({driver_filter})
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY i.session_key, i.driver_number, lb.lap_number
            ORDER BY i.recorded_at DESC
        ) = 1
        ORDER BY race_label, lb.lap_number, i.gap_to_leader_s
    """).to_pandas()

    if not interval_data.empty:
        for race_label in interval_data["RACE_LABEL"].unique():
            race_int = interval_data[interval_data["RACE_LABEL"] == race_label]
            st.caption(race_label)
            pivot = race_int.pivot_table(
                index="LAP_NUMBER", columns="DRIVER_ACRONYM", values="GAP_TO_LEADER_S"
            ).round(3)
            st.dataframe(
                pivot.style.format(lambda x: f"+{x:.3f}s" if pd.notna(x) and x > 0 else ("LEAD" if pd.notna(x) else "—")),
                use_container_width=True,
            )
    else:
        st.info("No interval data for selected sessions/drivers.")
