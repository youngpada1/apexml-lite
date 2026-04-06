import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from utils.colors import TEAM_COLORS

TYRE_COLORS = {
    "SOFT":         "#e8002d",
    "MEDIUM":       "#ffd700",
    "HARD":         "#c8c8c8",
    "INTERMEDIATE": "#39b54a",
    "WET":          "#0067ff",
}


def fmt_laptime(s):
    if pd.isna(s):
        return "—"
    m = int(s // 60)
    sec = s - m * 60
    return f"{m}:{sec:06.3f}"


def fmt_deg(s):
    if pd.isna(s):
        return "—"
    sign = "+" if s > 0 else ""
    return f"→ {sign}{s:.2f}s/lap"


def render(session, session_key: int):
    # Always use the Race session for strategy data
    race_session = session.sql(f"""
        SELECT s2.session_key
        FROM APEXML_DB.PROD.DIM_SESSIONS s1
        JOIN APEXML_DB.PROD.DIM_SESSIONS s2
            ON s1.meeting_key = s2.meeting_key
        WHERE s1.session_key = {session_key}
          AND s2.session_type = 'Race'
          AND s2.session_name = 'Race'
        LIMIT 1
    """).to_pandas()

    race_key = int(race_session.iloc[0]["SESSION_KEY"]) if not race_session.empty else session_key

    stints = session.sql(f"""
        SELECT driver_number, driver_name, driver_acronym, team_name,
               stint_number, tyre_compound, lap_start, lap_end, stint_length,
               clean_laps, fastest_lap_s, avg_lap_time_s, gap_to_best_s,
               deg_per_lap_s, deg_slope_s
        FROM APEXML_DB.PROD.FCT_STINTS
        WHERE session_key = {race_key}
        ORDER BY driver_acronym, stint_number
    """).to_pandas()

    # Get finish positions to sort drivers
    results = session.sql(f"""
        SELECT driver_acronym, finish_position
        FROM APEXML_DB.PROD.FCT_SESSION_RESULTS
        WHERE session_key = {race_key}
    """).to_pandas()

    if stints.empty:
        st.info("No strategy data for this session.")
        return

    # Merge finish position onto stints
    stints = stints.merge(results[["DRIVER_ACRONYM", "FINISH_POSITION"]], on="DRIVER_ACRONYM", how="left")
    stints["TYRE_COMPOUND"] = stints["TYRE_COMPOUND"].fillna("HARD").str.upper()

    # Driver order by finish position
    driver_order = (
        stints[["DRIVER_ACRONYM", "FINISH_POSITION"]]
        .drop_duplicates()
        .sort_values("FINISH_POSITION", na_position="last")
    )

    tab1, tab2 = st.tabs(["Overview", "Stint Detail"])

    # ── Tab 1: Overview chart ─────────────────────────────────────────────────
    with tab1:
        fig = go.Figure()

        # Legend tracking
        seen_compounds = set()

        for _, drow in driver_order.iterrows():
            acr = drow["DRIVER_ACRONYM"]
            pos = drow["FINISH_POSITION"]
            label = f"P{int(pos)} {acr}" if pd.notna(pos) else acr
            driver_stints = stints[stints["DRIVER_ACRONYM"] == acr].sort_values("STINT_NUMBER")

            for _, s in driver_stints.iterrows():
                compound = s["TYRE_COMPOUND"]
                color    = TYRE_COLORS.get(compound, "#888")
                length   = s["LAP_END"] - s["LAP_START"] + 1 if pd.notna(s["LAP_END"]) else s["STINT_LENGTH"]
                show_leg = compound not in seen_compounds
                seen_compounds.add(compound)

                fig.add_trace(go.Bar(
                    name=compound.capitalize(),
                    x=[length],
                    y=[label],
                    orientation="h",
                    marker_color=color,
                    showlegend=show_leg,
                    legendgroup=compound,
                    base=s["LAP_START"] - 1,
                    hovertemplate=(
                        f"<b>{acr}</b> — Stint {int(s['STINT_NUMBER'])}<br>"
                        f"{compound.capitalize()}<br>"
                        f"Laps {int(s['LAP_START']) if pd.notna(s['LAP_START']) else '?'}–{int(s['LAP_END']) if pd.notna(s['LAP_END']) else '?'} "
                        f"({int(length) if pd.notna(length) else '?'} laps)<extra></extra>"
                    ),
                ))

        fig.update_layout(
            barmode="overlay",
            template="plotly_dark",
            height=max(400, len(driver_order) * 28),
            xaxis=dict(title="Lap"),
            yaxis=dict(autorange="reversed", tickfont=dict(size=11)),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            margin=dict(l=80, r=20, t=40, b=40),
            paper_bgcolor="#0e1117",
            plot_bgcolor="#0e1117",
        )

        st.plotly_chart(fig, use_container_width=True)

    # ── Tab 2: Stint Detail ───────────────────────────────────────────────────
    with tab2:
        detail = stints.sort_values(["FINISH_POSITION", "STINT_NUMBER"], na_position="last").reset_index(drop=True).copy()

        display = pd.DataFrame()
        display["DRIVER"]   = detail["DRIVER_ACRONYM"]
        display["STINT"]    = detail["STINT_NUMBER"].apply(lambda x: int(x) if pd.notna(x) else "—")
        display["COMPOUND"] = detail["TYRE_COMPOUND"]
        display["LAPS"]     = detail.apply(lambda r: f"{int(r['STINT_LENGTH']) if pd.notna(r['STINT_LENGTH']) else '?'} ({int(r['LAP_START']) if pd.notna(r['LAP_START']) else '?'}–{int(r['LAP_END']) if pd.notna(r['LAP_END']) else '?'})" if pd.notna(r["LAP_END"]) else (str(int(r["STINT_LENGTH"])) if pd.notna(r["STINT_LENGTH"]) else "—"), axis=1)
        display["BEST"]     = detail["FASTEST_LAP_S"].apply(fmt_laptime)
        display["AVG"]      = detail["AVG_LAP_TIME_S"].apply(fmt_laptime)
        display["GAP"]      = detail["GAP_TO_BEST_S"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "—")
        display["DEG/LAP"]  = detail["DEG_PER_LAP_S"].apply(lambda x: f"{x:+.3f}" if pd.notna(x) else "—")

        def style_detail(df):
            styles = pd.DataFrame("", index=df.index, columns=df.columns)
            for i, row in df.iterrows():
                team     = detail.iloc[i]["TEAM_NAME"]
                compound = detail.iloc[i]["TYRE_COMPOUND"]
                color    = TEAM_COLORS.get(team, "#888")
                tcolor   = TYRE_COLORS.get(compound, "#888")
                styles.at[i, "DRIVER"]   = f"color: {color}; font-weight: 700; font-family: monospace;"
                styles.at[i, "COMPOUND"] = f"color: {tcolor}; font-weight: 700;"
                delta = detail.iloc[i]["DEG_PER_LAP_S"]
                if pd.notna(delta) and delta != 0:
                    styles.at[i, "DEG/LAP"] = "color: #ff4444; font-weight: 700;" if delta > 0 else "color: #00ff88; font-weight: 700;"
            return styles

        st.dataframe(
            display.style.apply(style_detail, axis=None),
            use_container_width=True,
            hide_index=True,
        )

        with st.expander("How to read this table"):
            st.markdown("""
- **Driver** — Driver acronym, coloured by team
- **Stint** — Stint number (1 = first stint, 2 = after first pit stop, etc.)
- **Compound** — Tyre compound used in that stint
- **Laps** — Total laps in that stint
- **Best** — Fastest clean lap in the stint (SC and pit out laps excluded)
- **Mean** — Average lap time across clean laps
- **Gap** — Mean minus Best: how far the average is from the fastest lap (higher = more inconsistent)
- **Deg/Lap** — Degradation per lap: (last lap − first lap) / stint length. Positive = getting slower, negative = improving.
""")
