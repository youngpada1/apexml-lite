import streamlit as st

st.set_page_config(
    page_title="ApexML-Lite",
    page_icon="🏎",
    layout="wide",
)

st.title("🏎 ApexML-Lite — F1 Analytics")

tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "🤖 Chatbot", "📈 Forecast"])

with tab1:
    from pages.dashboard import render
    render()

with tab2:
    from pages.chatbot import render
    render()

with tab3:
    from pages.forecast import render
    render()
