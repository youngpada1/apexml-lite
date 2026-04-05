import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from utils.colors import TEAM_COLORS


FLAG_COLORS = {
    "GREEN":      "#00ff88",
    "YELLOW":     "#ffd700",
    "DOUBLE YELLOW": "#ffd700",
    "RED":        "#ff4444",
    "BLUE":       "#4488ff",
    "WHITE":      "#ffffff",
    "ORANGE":     "#ff8c00",
    "BLACK":      "#888888",
    "CHEQUERED":  "#cccccc",
    "CLEAR":      "#00ff88",
}


def render(session, session_key: int):
    tab1, tab2, tab3 = st.tabs(["Race Positions", "Grid Comparison", "Race Control"])

    # ── Load lap timestamps ───────────────────────────────────────────────────
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
        with tab1:
            st.info("No position data for this session.")
        return

    # ── Map each position record to a lap number ──────────────────────────────
    if not laps.empty:
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

        positions = positions.sort_values("RECORDED_AT")
        positions = positions.groupby(["DRIVER_NUMBER", "LAP"], as_index=False).last()
    else:
        positions["LAP"] = positions.groupby("DRIVER_NUMBER").cumcount() + 1

    total_laps = int(positions["LAP"].max()) if not positions.empty else 0

    # ── Tab 1: Race Positions ─────────────────────────────────────────────────
    with tab1:
        all_drivers = sorted(positions["DRIVER_ACRONYM"].dropna().unique().tolist())
        selected = st.multiselect("Drivers", all_drivers, default=all_drivers, label_visibility="collapsed")

        if not selected:
            st.warning("Select at least one driver.")
        else:
            filtered = positions[positions["DRIVER_ACRONYM"].isin(selected)]

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

    # ── Tab 2: Grid Comparison ────────────────────────────────────────────────
    with tab2:
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

    # ── Tab 3: Race Control ───────────────────────────────────────────────────
    with tab3:
        rc = session.sql(f"""
            SELECT recorded_at, lap_number, flag, category, message, driver_name, driver_acronym, scope, sector
            FROM APEXML_DB.PROD.FCT_RACE_CONTROL
            WHERE session_key = {session_key}
            ORDER BY recorded_at
        """).to_pandas()

        if rc.empty:
            st.info("No race control data for this session.")
        else:
            # Normalise string columns
            rc["FLAG"]     = rc["FLAG"].apply(lambda x: str(x).upper().strip() if pd.notna(x) else "")
            rc["CATEGORY"] = rc["CATEGORY"].apply(lambda x: str(x).strip() if pd.notna(x) else "")

            # ── Filters ───────────────────────────────────────────────────
            total = len(rc)
            col_cat, col_flag = st.columns(2)

            all_categories = sorted(rc["CATEGORY"].unique().tolist())
            all_flags      = sorted([f for f in rc["FLAG"].unique().tolist() if f])

            with col_cat:
                sel_cats = st.multiselect("Filter by category", all_categories, default=all_categories, label_visibility="collapsed", placeholder="Filter by category")
            with col_flag:
                sel_flags = st.multiselect("Filter by flag", all_flags, default=all_flags, label_visibility="collapsed", placeholder="Filter by flag")

            filtered_rc = rc[
                rc["CATEGORY"].isin(sel_cats) &
                (rc["FLAG"].isin(sel_flags) | (rc["FLAG"] == ""))
            ]

            st.markdown(
                f"**Race Control Timeline** &nbsp; <span style='color:#aaa;font-size:13px;'>{len(filtered_rc)} of {total} events</span>",
                unsafe_allow_html=True,
            )

            # ── Event cards ───────────────────────────────────────────────
            for _, row in filtered_rc.iterrows():
                flag     = row["FLAG"]
                category = row["CATEGORY"]
                color    = FLAG_COLORS.get(flag, "#555555")
                lap_str  = f"Lap {int(row['LAP_NUMBER'])}" if pd.notna(row["LAP_NUMBER"]) else ""
                time_str = row["RECORDED_AT"].strftime("%H:%M:%S") if pd.notna(row["RECORDED_AT"]) else ""
                sector   = f"Sector {int(row['SECTOR'])}" if pd.notna(row.get("SECTOR")) else ""
                scope    = row.get("SCOPE", "") or ""
                scope    = str(scope).strip() if pd.notna(scope) else ""

                flag_badge     = f'<span style="background:{color};color:#000;padding:1px 7px;border-radius:3px;font-size:11px;font-weight:700;">{flag}</span>' if flag else ""
                cat_badge      = f'<span style="background:#2a2a2a;color:#ccc;padding:1px 7px;border-radius:3px;font-size:11px;">{category}</span>' if category else ""
                lap_badge      = f'<span style="background:#1e3a5f;color:#7eb8f7;padding:1px 7px;border-radius:3px;font-size:11px;font-weight:600;">{lap_str}</span>' if lap_str else ""
                sector_badge   = f'<span style="background:#2a2a2a;color:#aaa;padding:1px 7px;border-radius:3px;font-size:11px;">{sector}</span>' if sector else ""
                scope_badge    = f'<span style="background:#2a2a2a;color:#aaa;padding:1px 7px;border-radius:3px;font-size:11px;">{scope}</span>' if scope and scope.lower() not in ("nan", "") else ""

                badges = " ".join(b for b in [cat_badge, flag_badge, lap_badge] if b)
                meta   = " &nbsp; ".join(b for b in [
                    f'<span style="color:#888;font-size:11px;">🕐 {time_str}</span>' if time_str else "",
                    sector_badge,
                    scope_badge,
                ] if b)

                st.markdown(f"""
<div style="border:1px solid #2a2a2a;border-radius:6px;padding:10px 14px;margin-bottom:6px;background:#111;">
  <div style="margin-bottom:5px;">{badges}</div>
  <div style="font-weight:600;font-size:14px;color:#eee;">{row['MESSAGE']}</div>
  <div style="margin-top:4px;">{meta}</div>
</div>""", unsafe_allow_html=True)
