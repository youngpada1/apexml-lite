import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from utils.colors import TEAM_COLORS

os.makedirs("/tmp/fastf1_cache", exist_ok=True)
import fastf1
fastf1.Cache.enable_cache("/tmp/fastf1_cache")


@st.cache_data(show_spinner=False)
def get_circuit_map(year: int, event_name: str):
    try:
        ff1 = fastf1.get_session(year, event_name, "R")
        ff1.load(telemetry=True, weather=False, messages=False, laps=True)
        lap = ff1.laps.pick_fastest()
        pos = lap.get_pos_data()[["X", "Y"]].interpolate().dropna()
        x, y = pos["X"].values, pos["Y"].values
        circuit_info = ff1.get_circuit_info()
        # FastF1 rotation is in degrees; negative sign needed to match F1 map orientation
        angle = np.radians(-circuit_info.rotation)
        xr = x * np.cos(angle) - y * np.sin(angle)
        yr = x * np.sin(angle) + y * np.cos(angle)
        return xr, yr
    except Exception:
        return None, None


def get_fastest_lap(laps_df, acronym):
    d = laps_df[laps_df["DRIVER_ACRONYM"] == acronym]
    if d.empty:
        return None
    return int(d.loc[d["LAP_DURATION_S"].idxmin(), "LAP_NUMBER"])


def load_telemetry(session, session_key, driver_number, lap_start_at, lap_end_at):
    return session.sql(f"""
        SELECT c.recorded_at, c.speed_kmh, c.throttle_pct, c.is_braking, c.drs_status, c.rpm,
               l.pos_x, l.pos_y
        FROM APEXML_DB.PROD.FCT_CAR_DATA c
        ASOF JOIN APEXML_DB.PROD.FCT_LOCATION l
            MATCH_CONDITION (c.recorded_at >= l.recorded_at)
            ON  c.session_key   = l.session_key
            AND c.driver_number = l.driver_number
        WHERE c.session_key   = {session_key}
          AND c.driver_number = {driver_number}
          AND c.recorded_at  >= '{lap_start_at}'
          AND c.recorded_at  <  '{lap_end_at}'
        ORDER BY c.recorded_at
    """).to_pandas()


@st.cache_data(ttl=86400, show_spinner=False)
def _get_meta(_session, session_key: int):
    return _session.sql(f"""
        SELECT year, meeting_name FROM APEXML_DB.PROD.DIM_SESSIONS
        WHERE session_key = {session_key} LIMIT 1
    """).to_pandas()


@st.cache_data(ttl=86400, show_spinner=False)
def _get_laps(_session, session_key: int):
    return _session.sql(f"""
        SELECT l.lap_number, l.lap_duration_s, l.lap_start_at,
               DATEADD('second', l.lap_duration_s, l.lap_start_at) AS lap_end_at,
               l.driver_number,
               d.acronym AS driver_acronym, d.full_name AS driver_name, d.team_name
        FROM APEXML_DB.PROD.FCT_LAPS l
        JOIN APEXML_DB.PROD.DIM_DRIVERS d
            ON l.session_key = d.session_key AND l.driver_number = d.driver_number
        WHERE l.session_key = {session_key}
          AND l.lap_duration_s IS NOT NULL
          AND l.lap_duration_s < 120
          AND l.is_pit_out_lap = false
        ORDER BY l.lap_number
    """).to_pandas()


def render(session, session_key: int):
    # ── Session metadata for FastF1 ───────────────────────────────────────────
    meta      = _get_meta(session, session_key)
    ff1_year  = int(meta["YEAR"].iloc[0])        if not meta.empty else None
    ff1_event = str(meta["MEETING_NAME"].iloc[0]) if not meta.empty else None

    laps = _get_laps(session, session_key)

    if laps.empty:
        st.info("No lap data for this session.")
        return

    all_drivers = sorted(laps["DRIVER_ACRONYM"].dropna().unique().tolist())

    # ── Layout ────────────────────────────────────────────────────────────────
    col_charts, col_sel = st.columns([4, 1])

    with col_sel:
        st.caption("Drivers")
        selected_drivers = st.multiselect(
            "Drivers", all_drivers, default=all_drivers[:2],
            key="td_drivers_multi", label_visibility="collapsed"
        )

        selected_combos = []
        for drv in (selected_drivers or all_drivers[:2]):
            lap_opts = sorted(laps[laps["DRIVER_ACRONYM"] == drv]["LAP_NUMBER"].tolist())
            fastest = get_fastest_lap(laps, drv)
            def_idx = lap_opts.index(fastest) if fastest in lap_opts else 0
            st.caption(drv)
            lap = st.selectbox(
                f"Lap {drv}", lap_opts, index=def_idx,
                key=f"td_lap_{drv}", label_visibility="collapsed"
            )
            selected_combos.append(f"{drv} L{lap}")

    # ── Load telemetry for all selected combos ────────────────────────────────
    tel_all = {}
    active = selected_combos or []
    for combo in active:
        try:
            drv, lap_str = combo.rsplit(" L", 1)
            lap_num = int(lap_str)
        except ValueError:
            continue
        rows = laps[(laps["DRIVER_ACRONYM"] == drv) & (laps["LAP_NUMBER"] == lap_num)]
        if rows.empty:
            continue
        row = rows.iloc[0]
        tel = load_telemetry(session, session_key, int(row["DRIVER_NUMBER"]),
                             row["LAP_START_AT"], row["LAP_END_AT"])
        if not tel.empty:
            tel = tel.reset_index(drop=True)
            tel["DIST"] = tel.index
            tel_all[combo] = {
                "tel": tel,
                "team": row["TEAM_NAME"],
                "lap": lap_num,
                "time": f"{row['LAP_DURATION_S']:.3f}s",
                "color": TEAM_COLORS.get(row["TEAM_NAME"], "#888888"),
            }

    with col_charts:
        if not tel_all:
            st.warning("No telemetry data for the selected drivers.")
            return

        # ── Track map + Speed trace ───────────────────────────────────────────
        st.markdown("#### Track Dominance")
        caption = "  vs  ".join(f"{c} ({d['time']})" for c, d in tel_all.items())
        st.caption(caption)

        col_map, col_speed = st.columns([1, 1])

        with col_map:
            fig_map = go.Figure()

            ff1_x, ff1_y = get_circuit_map(ff1_year, ff1_event) if ff1_year else (None, None)
            use_ff1 = ff1_x is not None and len(ff1_x) > 10

            # Interpolate every driver's speed onto the map grid
            combos = list(tel_all.keys())
            if use_ff1:
                n = len(ff1_x)
                x, y = ff1_x, ff1_y
            else:
                # Fall back: use pos_x/pos_y from first driver
                loc0 = tel_all[combos[0]]["tel"].dropna(subset=["POS_X", "POS_Y"])
                if loc0.empty:
                    x, y = np.array([]), np.array([])
                else:
                    x, y = loc0["POS_X"].values, loc0["POS_Y"].values
                n = len(x)

            if n > 1 and len(x) > 1:
                # Build speed matrix: shape (n_combos, n)
                speeds = np.stack([
                    np.interp(
                        np.linspace(0, len(d["tel"]) - 1, n),
                        np.arange(len(d["tel"])),
                        d["tel"]["SPEED_KMH"].fillna(0).values
                    )
                    for d in tel_all.values()
                ])  # shape: (n_combos, n)

                winner_idx = np.argmax(speeds, axis=0)  # index of fastest driver at each point

                for i in range(len(x) - 1):
                    w = winner_idx[i]
                    seg_color = list(tel_all.values())[w]["color"]
                    fig_map.add_trace(go.Scatter(
                        x=[x[i], x[i+1]], y=[y[i], y[i+1]],
                        mode="lines", line=dict(color=seg_color, width=3),
                        showlegend=False, hoverinfo="skip",
                    ))

                # Legend entries
                for combo, data in tel_all.items():
                    fig_map.add_trace(go.Scatter(
                        x=[None], y=[None], mode="lines",
                        line=dict(color=data["color"], width=3),
                        name=combo,
                    ))

            fig_map.update_layout(
                template="plotly_dark", height=400,
                xaxis=dict(visible=False, scaleanchor="y", scaleratio=1),
                yaxis=dict(visible=False),
                legend=dict(orientation="h", yanchor="top", y=-0.02, xanchor="center", x=0.5, font=dict(size=11)),
                margin=dict(l=0, r=0, t=10, b=60),
                paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
            )
            st.plotly_chart(fig_map, use_container_width=True)

        with col_speed:
            fig_spd = go.Figure()
            for combo, data in tel_all.items():
                fig_spd.add_trace(go.Scatter(
                    x=data["tel"]["DIST"], y=data["tel"]["SPEED_KMH"],
                    mode="lines", name=combo,
                    line=dict(color=data["color"], width=2),
                ))
            fig_spd.update_layout(
                template="plotly_dark", height=400,
                xaxis=dict(title="Distance (samples)"),
                yaxis=dict(title="Speed (km/h)"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                margin=dict(l=60, r=20, t=30, b=40),
                paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
            )
            st.plotly_chart(fig_spd, use_container_width=True)

        # ── Telemetry 2x2 ─────────────────────────────────────────────────────
        st.markdown("#### Telemetry Comparison")
        metrics = [
            ("Throttle (%)", "THROTTLE_PCT", "Throttle (%)"),
            ("Braking",      "IS_BRAKING",   "Braking"),
            ("RPM",          "RPM",           "RPM"),
            ("DRS",          "DRS_STATUS",    "DRS"),
        ]
        row1 = st.columns(2)
        row2 = st.columns(2)
        for col, (title, metric_col, ylabel) in zip([*row1, *row2], metrics):
            with col:
                fig = go.Figure()
                for combo, data in tel_all.items():
                    if metric_col in data["tel"].columns:
                        fig.add_trace(go.Scatter(
                            x=data["tel"]["DIST"], y=data["tel"][metric_col],
                            mode="lines", name=combo,
                            line=dict(color=data["color"], width=1.5),
                        ))
                fig.update_layout(
                    title=dict(text=title, font=dict(size=12), x=0.01),
                    template="plotly_dark", height=220,
                    xaxis=dict(title="", tickfont=dict(size=9), showgrid=False),
                    yaxis=dict(title=ylabel, tickfont=dict(size=9)),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0, font=dict(size=9)),
                    margin=dict(l=45, r=10, t=35, b=25),
                    paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                )
                st.plotly_chart(fig, use_container_width=True)
