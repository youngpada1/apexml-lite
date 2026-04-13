import streamlit as st
import pandas as pd
from utils.colors import TEAM_COLORS


@st.cache_data(ttl=86400, show_spinner=False)
def _get_sessions(_session, meeting_key: int):
    return _session.sql(f"""
        SELECT session_key, session_name, session_start_at, meeting_name, year
        FROM APEXML_DB.PROD.DIM_SESSIONS
        WHERE meeting_key = {meeting_key}
        ORDER BY session_start_at
    """).to_pandas()


@st.cache_data(ttl=86400, show_spinner=False)
def _get_hero(_session, session_key: int):
    return _session.sql(f"""
        SELECT
            MAX(CASE WHEN finish_position = 1 THEN driver_name END) AS race_winner,
            MAX(CASE WHEN finish_position = 1 THEN team_name  END) AS winner_team
        FROM APEXML_DB.PROD.FCT_SESSION_RESULTS
        WHERE session_key = {session_key}
    """).to_pandas().iloc[0]


@st.cache_data(ttl=86400, show_spinner=False)
def _get_fastest(_session, session_key: int):
    return _session.sql(f"""
        SELECT driver_name, MIN(lap_duration_s) AS fl
        FROM APEXML_DB.PROD.FCT_LAPS
        WHERE session_key = {session_key}
        GROUP BY driver_name ORDER BY fl LIMIT 1
    """).to_pandas()


@st.cache_data(ttl=86400, show_spinner=False)
def _get_pole(_session, session_key: int):
    return _session.sql(f"""
        SELECT r.driver_name
        FROM APEXML_DB.PROD.FCT_SESSION_RESULTS r
        WHERE r.session_key = {session_key}
        AND r.grid_position = 1
        LIMIT 1
    """).to_pandas()


def fmt_laptime(seconds):
    if seconds is None or (isinstance(seconds, float) and pd.isna(seconds)):
        return "—"
    m = int(seconds // 60)
    s = seconds % 60
    return f"{m}:{s:06.3f}"


def render(session):
    meeting_key = st.session_state.get("selected_meeting_key")
    session_key = st.session_state.get("selected_session_key")

    if not meeting_key or not session_key:
        st.warning("No race selected.")
        if st.button("← Back to Calendar"):
            st.session_state["page"] = "calendar"
            st.rerun()
        return

    if st.button("← Calendar"):
        st.session_state["page"] = "calendar"
        st.rerun()

    # ── Session selector ──────────────────────────────────────────────────────
    sessions_df = _get_sessions(session, meeting_key)

    session_options = sessions_df["SESSION_NAME"].tolist()
    current_pos = sessions_df[sessions_df["SESSION_KEY"] == session_key].index
    current_pos = sessions_df.index.get_loc(current_pos[0]) if len(current_pos) > 0 else 0

    col_title, col_sel = st.columns([3, 1])
    with col_title:
        meeting_name = sessions_df["MEETING_NAME"].iloc[0]
        year = int(sessions_df["YEAR"].iloc[0])
        current_name = sessions_df[sessions_df["SESSION_KEY"] == session_key]["SESSION_NAME"].values[0]
        st.subheader(f"{meeting_name} — {current_name}")
        st.caption(f"{year} Season")

    with col_sel:
        selected_name = st.selectbox("Session", session_options, index=current_pos, label_visibility="collapsed")
        new_key = int(sessions_df[sessions_df["SESSION_NAME"] == selected_name]["SESSION_KEY"].values[0])
        if new_key != session_key:
            st.session_state["selected_session_key"] = new_key
            st.rerun()

    # ── Hero cards ────────────────────────────────────────────────────────────
    hero    = _get_hero(session, session_key)
    fastest = _get_fastest(session, session_key)
    pole    = _get_pole(session, session_key)

    c1, c2, c3 = st.columns(3)
    with c1:
        w = hero["RACE_WINNER"] or "—"
        wt = hero["WINNER_TEAM"] or ""
        st.metric("🏆 Race Winner", w, delta=wt, delta_color="off")
    with c2:
        p = pole["DRIVER_NAME"].iloc[0] if not pole.empty else "—"
        st.metric("⚡ Pole Position", p)
    with c3:
        if not fastest.empty:
            st.metric("⏱ Fastest Lap", fastest["DRIVER_NAME"].iloc[0], delta=fmt_laptime(fastest["FL"].iloc[0]), delta_color="off")
        else:
            st.metric("⏱ Fastest Lap", "—")

    st.divider()

    # ── Tab navigation ────────────────────────────────────────────────────────
    from tabs.results         import render as render_results
    from tabs.positions       import render as render_positions
    from tabs.strategy        import render as render_strategy
    from tabs.lap_times       import render as render_lap_times
    from tabs.track_dominance import render as render_track_dominance

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📋 Results", "📍 Positions", "🏁 Strategy",
        "⏱ Lap Times", "🗺 Track Dominance"
    ])
    with tab1: render_results(session, session_key)
    with tab2: render_positions(session, session_key)
    with tab3: render_strategy(session, session_key)
    with tab4: render_lap_times(session, session_key)
    with tab5: render_track_dominance(session, session_key)
