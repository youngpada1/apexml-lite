import streamlit as st
from utils.connection import get_session
from tabs.dashboard import render as analytics
from tabs.chatbot import render as chatbot
from tabs.forecast import render as forecast

st.set_page_config(
    page_title="ApexML-Lite",
    page_icon="🏎",
    layout="wide",
)

st.title("🏎 ApexML-Lite — F1 Analytics")

session = get_session()

tab1, tab2, tab3 = st.tabs(["📊 Analytics", "🤖 Chatbot", "📈 Forecast"])

with tab1:
    analytics(session)

with tab2:
    chatbot(session)

with tab3:
    forecast(session)
