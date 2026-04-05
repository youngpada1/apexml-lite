import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from utils.colors import TEAM_COLORS


def render(session, session_key: int):
    laps = session.sql(f"""
        SELECT l.lap_number, l.lap_duration_s, l.is_pit_out_lap,
               d.acronym AS driver_acronym, d.full_name AS driver_name, d.team_name
        FROM APEXML_DB.PROD.FCT_LAPS l
        JOIN APEXML_DB.PROD.DIM_DRIVERS d
            ON l.session_key = d.session_key
            AND l.driver_number = d.driver_number
        WHERE l.session_key = {session_key}
          AND l.lap_duration_s IS NOT NULL
        ORDER BY l.lap_number
    """).to_pandas()

    if laps.empty:
        st.info("No lap time data for this session.")
        return

    all_drivers = sorted(laps["DRIVER_ACRONYM"].dropna().unique().tolist())

    col_sel, col_filter = st.columns([4, 1])
    with col_sel:
        selected = st.multiselect(
            "Drivers", all_drivers, default=all_drivers[:2],
            label_visibility="collapsed"
        )
    with col_filter:
        exclude_sc = st.toggle("Exclude SC laps", value=True)

    if not selected:
        st.warning("Select at least one driver.")
        return

    filtered = laps[laps["DRIVER_ACRONYM"].isin(selected)].copy()

    if exclude_sc:
        filtered = filtered[
            (filtered["IS_PIT_OUT_LAP"] == False) &
            (filtered["LAP_DURATION_S"] < 120)
        ]

    fig = go.Figure()

    for driver in selected:
        df = filtered[filtered["DRIVER_ACRONYM"] == driver].sort_values("LAP_NUMBER")
        if df.empty:
            continue
        team  = df["TEAM_NAME"].iloc[0]
        name  = df["DRIVER_NAME"].iloc[0]
        color = TEAM_COLORS.get(team, "#888")

        fig.add_trace(go.Scatter(
            x=df["LAP_NUMBER"],
            y=df["LAP_DURATION_S"],
            mode="lines+markers",
            name=driver,
            line=dict(color=color, width=2),
            marker=dict(size=4),
            hovertemplate=f"<b>{name}</b><br>Lap %{{x}}<br>%{{y:.3f}}s<extra></extra>",
        ))

    fig.update_layout(
        template="plotly_dark",
        height=500,
        xaxis=dict(title="Lap"),
        yaxis=dict(title="Lap Time (s)"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=60, r=20, t=40, b=40),
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
    )

    st.plotly_chart(fig, use_container_width=True)
