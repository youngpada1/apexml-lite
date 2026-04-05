import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from utils.colors import TEAM_COLORS


def render(session, session_key: int):
    st.markdown("#### Race Positions")

    # ── Load lap timestamps to map recorded_at → lap number ──────────────────
    laps = session.sql(f"""
        SELECT driver_number, lap_number, lap_start_at,
               DATEADD('second', lap_duration_s, lap_start_at) AS lap_end_at
        FROM APEXML_DB.PROD.FCT_LAPS
        WHERE session_key = {session_key}
        ORDER BY driver_number, lap_number
    """).to_pandas()

    positions = session.sql(f"""
        SELECT driver_number, driver_name, driver_acronym, team_name,
               position, recorded_at, grid_position, finish_position, positions_gained
        FROM APEXML_DB.PROD.FCT_RACE_POSITIONS
        WHERE session_key = {session_key}
        ORDER BY recorded_at
    """).to_pandas()

    if positions.empty:
        st.info("No position data for this session.")
        return

    # ── Map each position record to a lap number ──────────────────────────────
    if not laps.empty:
        # Build a lap boundary lookup per driver
        laps["LAP_START_AT"] = pd.to_datetime(laps["LAP_START_AT"], utc=True)
        laps["LAP_END_AT"]   = pd.to_datetime(laps["LAP_END_AT"],   utc=True)
        positions["RECORDED_AT"] = pd.to_datetime(positions["RECORDED_AT"], utc=True)

        def get_lap(row):
            driver_laps = laps[laps["DRIVER_NUMBER"] == row["DRIVER_NUMBER"]]
            mask = (driver_laps["LAP_START_AT"] <= row["RECORDED_AT"]) & \
                   (row["RECORDED_AT"] <= driver_laps["LAP_END_AT"])
            matched = driver_laps[mask]
            return int(matched["LAP_NUMBER"].iloc[0]) if not matched.empty else None

        positions["LAP"] = positions.apply(get_lap, axis=1)
        positions = positions.dropna(subset=["LAP"])
        positions["LAP"] = positions["LAP"].astype(int)

        # One position per driver per lap (take last recorded position in that lap)
        positions = positions.sort_values("RECORDED_AT")
        positions = positions.groupby(["DRIVER_NUMBER", "LAP"], as_index=False).last()
    else:
        # Fallback: use row number as lap proxy
        positions["LAP"] = positions.groupby("DRIVER_NUMBER").cumcount() + 1

    total_laps = int(positions["LAP"].max()) if not positions.empty else 0

    # ── Driver selector ───────────────────────────────────────────────────────
    all_drivers = sorted(positions["DRIVER_ACRONYM"].dropna().unique().tolist())
    selected = st.multiselect("Drivers", all_drivers, default=all_drivers, label_visibility="collapsed")

    if not selected:
        st.warning("Select at least one driver.")
        return

    filtered = positions[positions["DRIVER_ACRONYM"].isin(selected)]

    # ── Plotly position chart ─────────────────────────────────────────────────
    fig = go.Figure()

    for driver in filtered["DRIVER_ACRONYM"].unique():
        df = filtered[filtered["DRIVER_ACRONYM"] == driver].sort_values("LAP")
        team  = df["TEAM_NAME"].iloc[0]
        color = TEAM_COLORS.get(team, "#888")
        name  = df["DRIVER_NAME"].iloc[0]

        fig.add_trace(go.Scatter(
            x=df["LAP"],
            y=df["POSITION"],
            mode="lines",
            name=driver,
            line=dict(color=color, width=2),
            hovertemplate=f"<b>{name}</b><br>Lap %{{x}}<br>P%{{y}}<extra></extra>",
        ))

    fig.update_layout(
        template="plotly_dark",
        height=500,
        xaxis=dict(title="Lap", range=[1, total_laps]),
        yaxis=dict(title="Position", autorange="reversed", dtick=1),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=40, r=20, t=40, b=40),
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
    )

    st.plotly_chart(fig, use_container_width=True)

    # ── Places gained/lost table ──────────────────────────────────────────────
    st.markdown("#### Places Gained / Lost")

    summary = positions.groupby(
        ["DRIVER_ACRONYM", "DRIVER_NAME", "TEAM_NAME", "GRID_POSITION", "FINISH_POSITION", "POSITIONS_GAINED"],
        as_index=False
    ).size().drop(columns="size")

    summary = summary.sort_values("FINISH_POSITION")

    display = pd.DataFrame()
    display["DRIVER"]   = summary["DRIVER_NAME"]
    display["ACR"]      = summary["DRIVER_ACRONYM"]
    display["TEAM"]     = summary["TEAM_NAME"]
    display["GRID"]     = summary["GRID_POSITION"].apply(lambda x: int(x) if pd.notna(x) else "—")
    display["FINISH"]   = summary["FINISH_POSITION"].apply(lambda x: int(x) if pd.notna(x) else "—")
    display["+/−"]      = summary["POSITIONS_GAINED"].apply(
        lambda x: f"+{int(x)}" if pd.notna(x) and x > 0 else (str(int(x)) if pd.notna(x) and x != 0 else "—")
    )

    def style_summary(df):
        styles = pd.DataFrame("", index=df.index, columns=df.columns)
        for i, row in df.iterrows():
            color = TEAM_COLORS.get(summary.iloc[i]["TEAM_NAME"], "#888")
            styles.at[i, "ACR"]  = f"color: {color}; font-weight: 700; font-family: monospace;"
            styles.at[i, "TEAM"] = f"color: {color}; font-size: 11px;"
            gained = summary.iloc[i]["POSITIONS_GAINED"]
            if pd.notna(gained):
                if gained > 0:
                    styles.at[i, "+/−"] = "color: #00ff88; font-weight: 700;"
                elif gained < 0:
                    styles.at[i, "+/−"] = "color: #ff4444; font-weight: 700;"
        return styles

    st.dataframe(
        display.style.apply(style_summary, axis=None),
        use_container_width=True,
        hide_index=True,
    )