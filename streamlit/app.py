import streamlit as st
from utils.connection import get_session

st.set_page_config(
    page_title="ApexML-Lite — F1 Analytics",
    page_icon="🏎",
    layout="wide",
)

session = get_session()

# ── Init session state ────────────────────────────────────────────────────────
if "page" not in st.session_state:
    st.session_state["page"] = "calendar"

# ── Top navigation ────────────────────────────────────────────────────────────
col_logo, col_nav = st.columns([2, 3])
with col_logo:
    st.markdown("### 🏎 ApexML-Lite")
with col_nav:
    nav = st.radio(
        "nav", ["Race Calendar & Results", "Chatbot", "Forecast"],
        horizontal=True,
        label_visibility="collapsed",
        index=["calendar", "chatbot", "forecast"].index(
            st.session_state["page"] if st.session_state["page"] in ["calendar", "chatbot", "forecast"] else "calendar"
        ),
    )
    if nav == "Race Calendar & Results" and st.session_state["page"] not in ("calendar", "race"):
        st.session_state["page"] = "calendar"
        st.rerun()
    elif nav == "Chatbot" and st.session_state["page"] != "chatbot":
        st.session_state["page"] = "chatbot"
        st.rerun()
    elif nav == "Forecast" and st.session_state["page"] != "forecast":
        st.session_state["page"] = "forecast"
        st.rerun()

st.divider()

# ── Page routing ──────────────────────────────────────────────────────────────
page = st.session_state["page"]

if page == "calendar":
    from tabs.calendar import render
    render(session)

elif page == "race":
    from tabs.race import render
    render(session)

elif page == "chatbot":
    from tabs.chatbot import render
    render(session)

elif page == "forecast":
    from tabs.forecast import render
    render(session)
