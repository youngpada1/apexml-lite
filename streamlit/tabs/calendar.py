import streamlit as st
import pandas as pd
from datetime import datetime, timezone

FLAG_EMOJI = {
    "Australia": "🇦🇺", "China": "🇨🇳", "Japan": "🇯🇵",
    "Bahrain": "🇧🇭", "Saudi Arabia": "🇸🇦", "United States": "🇺🇸",
    "Miami": "🇺🇸", "Emilia Romagna": "🇮🇹", "Monaco": "🇲🇨",
    "Spain": "🇪🇸", "Canada": "🇨🇦", "Austria": "🇦🇹",
    "Great Britain": "🇬🇧", "Hungary": "🇭🇺", "Belgium": "🇧🇪",
    "Netherlands": "🇳🇱", "Italy": "🇮🇹", "Azerbaijan": "🇦🇿",
    "Singapore": "🇸🇬", "Mexico": "🇲🇽", "Brazil": "🇧🇷",
    "Las Vegas": "🇺🇸", "Qatar": "🇶🇦", "Abu Dhabi": "🇦🇪",
}


def render(session):
    col_title, col_sel = st.columns([3, 1])

    with col_title:
        st.subheader("Race Calendar & Results")

    years = session.sql(
        "SELECT DISTINCT year FROM APEXML_DB.PROD.DIM_SESSIONS ORDER BY year DESC"
    ).to_pandas()["YEAR"].tolist()

    with col_sel:
        selected_year = st.selectbox("Season", years, index=0, label_visibility="collapsed")

    st.caption(f"{selected_year} Season Overview")

    races = session.sql(f"""
        WITH race_sessions AS (
            SELECT
                s.session_key, s.meeting_key, s.meeting_name,
                s.circuit_short_name, s.country_name, s.session_start_at,
                ROW_NUMBER() OVER (PARTITION BY s.meeting_key ORDER BY s.session_start_at) AS rn,
                RANK() OVER (ORDER BY s.session_start_at) AS round_num
            FROM APEXML_DB.PROD.DIM_SESSIONS s
            WHERE s.year = {selected_year} AND s.session_type = 'Race' AND s.session_name = 'Race'
        ),
        winners AS (
            SELECT session_key, driver_name, team_name
            FROM APEXML_DB.PROD.FCT_SESSION_RESULTS
            WHERE finish_position = 1
        )
        SELECT
            rs.session_key, rs.meeting_key, rs.meeting_name,
            rs.circuit_short_name, rs.country_name, rs.session_start_at,
            rs.round_num,
            w.driver_name AS winner_name, w.team_name AS winner_team
        FROM race_sessions rs
        LEFT JOIN winners w ON rs.session_key = w.session_key
        WHERE rs.rn = 1
        ORDER BY rs.session_start_at
    """).to_pandas()

    now = datetime.now(timezone.utc)
    cols = st.columns(3)

    for i, (_, row) in enumerate(races.iterrows()):
        with cols[i % 3]:
            session_dt = row["SESSION_START_AT"]
            if pd.notna(session_dt) and hasattr(session_dt, "tzinfo") and session_dt.tzinfo is None:
                session_dt = session_dt.replace(tzinfo=timezone.utc)

            has_winner = pd.notna(row.get("WINNER_NAME"))
            status = "✅ COMPLETED" if has_winner else ("🕐 UPCOMING" if not pd.notna(session_dt) or session_dt > now else "✅ COMPLETED")
            date_str = session_dt.strftime("%b %d, %Y") if pd.notna(session_dt) else "TBD"
            flag = FLAG_EMOJI.get(row["COUNTRY_NAME"], "🏁")

            with st.container(border=True):
                st.markdown(f"**Round {int(row['ROUND_NUM'])} — {flag} {row['MEETING_NAME']}**")
                st.caption(f"{row['CIRCUIT_SHORT_NAME']} · {date_str} · {status}")
                if has_winner:
                    st.markdown(f"🏆 **{row['WINNER_NAME']}** — {row['WINNER_TEAM']}")
                else:
                    st.caption("No result yet")
                if st.button("View →", key=f"race_{row['SESSION_KEY']}_{i}"):
                    st.session_state["selected_meeting_key"] = int(row["MEETING_KEY"])
                    st.session_state["selected_session_key"] = int(row["SESSION_KEY"])
                    st.session_state["page"] = "race"
                    st.rerun()
