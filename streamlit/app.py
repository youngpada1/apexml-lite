import streamlit as st
from utils.connection import get_session
from tabs.dashboard import render as dashboard
from tabs.comparison import render as comparison
from tabs.chatbot import render as chatbot
from tabs.forecast import render as forecast

st.set_page_config(
    page_title="ApexML-Lite",
    page_icon="🏎",
    layout="wide",
)

st.title("🏎 ApexML-Lite — F1 Analytics")

session = get_session()

tab1, tab2, tab3, tab4 = st.tabs(["📊 Dashboard", "🔀 Comparison", "🤖 Chatbot", "📈 Forecast"])

with tab1:
    dashboard(session)

with tab2:
    comparison(session)

with tab3:
    chatbot(session)

with tab4:
    forecast(session)
