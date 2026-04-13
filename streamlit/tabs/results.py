import streamlit as st
import pandas as pd
from utils.colors import TEAM_COLORS


@st.cache_data(ttl=86400, show_spinner=False)
def _get_results(_session, session_key: int):
    return _session.sql(f"""
        SELECT finish_position, driver_name, driver_acronym, team_name,
               grid_position, classified_position, points
        FROM APEXML_DB.PROD.FCT_SESSION_RESULTS
        WHERE session_key = {session_key}
        ORDER BY finish_position
    """).to_pandas()


def render(session, session_key: int):
    st.markdown("#### Session Results")

    results = _get_results(session, session_key)

    if results.empty:
        st.info("No results data for this session.")
        return

    display = pd.DataFrame()
    display["POS"]    = results["FINISH_POSITION"].apply(lambda x: int(x) if pd.notna(x) else "—")
    display["DRIVER"] = results["DRIVER_NAME"]
    display["ACR"]    = results["DRIVER_ACRONYM"]
    display["TEAM"]   = results["TEAM_NAME"]
    display["GRID"]   = results["GRID_POSITION"].apply(lambda x: int(x) if pd.notna(x) else "—")
    display["STATUS"] = results["CLASSIFIED_POSITION"].fillna("Classified")
    display["PTS"]    = results["POINTS"].apply(lambda x: int(x) if pd.notna(x) and x > 0 else 0)

    def style_results(df):
        styles = pd.DataFrame("", index=df.index, columns=df.columns)
        for i, row in df.iterrows():
            team_color = TEAM_COLORS.get(results.iloc[i]["TEAM_NAME"], "#888")
            styles.at[i, "ACR"]    = f"color: {team_color}; font-weight: 700; font-family: monospace;"
            styles.at[i, "TEAM"]   = f"color: {team_color}; font-size: 11px;"
            styles.at[i, "POS"]    = "font-weight: 700; color: #fff;"
            styles.at[i, "PTS"]    = "color: #FFF200; font-weight: 700;"
            styles.at[i, "GRID"]   = "color: #888; font-family: monospace;"
        return styles

    st.dataframe(
        display.style.apply(style_results, axis=None),
        use_container_width=True,
        hide_index=True,
    )
