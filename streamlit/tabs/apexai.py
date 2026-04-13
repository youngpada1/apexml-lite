import os

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

from utils.jwt_generator import JWTGenerator

SEMANTIC_MODEL = "@APEXML_DB.PROD.CORTEX_STAGE/cortex_model.yaml"
ANALYST_ENDPOINT = "https://{account}.snowflakecomputing.com/api/v2/cortex/analyst/message"
COMPLETE_ENDPOINT = "https://{account}.snowflakecomputing.com/api/v2/cortex/v1/chat/completions"


@st.cache_resource
def _get_jwt_generator():
    return JWTGenerator(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_path=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
    )


def _headers() -> dict:
    token = _get_jwt_generator().get_token()
    return {
        "Authorization": f"Bearer {token}",
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _account_url(template: str) -> str:
    return template.format(account=os.environ["SNOWFLAKE_ACCOUNT"].lower())


def _call_analyst(messages: list) -> dict:
    resp = requests.post(
        _account_url(ANALYST_ENDPOINT),
        headers=_headers(),
        json={"messages": messages, "semantic_model_file": SEMANTIC_MODEL},
        timeout=60,
    )
    if resp.status_code == 401:
        _get_jwt_generator().token = None
        resp = requests.post(
            _account_url(ANALYST_ENDPOINT),
            headers=_headers(),
            json={"messages": messages, "semantic_model_file": SEMANTIC_MODEL},
            timeout=60,
        )
    resp.raise_for_status()
    return resp.json()


def _call_complete(prompt: str) -> str:
    resp = requests.post(
        _account_url(COMPLETE_ENDPOINT),
        headers=_headers(),
        json={
            "model": "claude-sonnet-4-5",
            "messages": [
                {"role": "system", "content": "You are an F1 data analyst. Summarise the following query results concisely in 2-3 sentences. Focus on the key insight, not the data format."},
                {"role": "user", "content": prompt},
            ],
            "max_completion_tokens": 256,
            "temperature": 0.3,
        },
        timeout=30,
    )
    if resp.status_code != 200:
        return ""
    return resp.json()["choices"][0]["message"]["content"]


COLUMN_LABELS = {
    "MEETING_NAME":               "Race",
    "COUNTRY_NAME":               "Country",
    "CIRCUIT_SHORT_NAME":         "Circuit",
    "SESSION_START_AT":           "Date & Time",
    "SESSION_END_AT":             "End Time",
    "SESSION_NAME":               "Session",
    "SESSION_TYPE":               "Type",
    "DRIVER_NAME":                "Driver",
    "DRIVER_ACRONYM":             "Code",
    "TEAM_NAME":                  "Team",
    "FINISH_POSITION":            "Pos",
    "GRID_POSITION":              "Grid",
    "POINTS":                     "Points",
    "CHAMPIONSHIP_POSITION":      "Standing",
    "POINTS_CURRENT":             "Points",
    "FULL_NAME":                  "Driver",
    "LAP_DURATION_S":             "Lap Time (s)",
    "FASTEST_LAP_S":              "Fastest Lap (s)",
    "AVG_LAP_TIME_S":             "Avg Lap Time (s)",
    "PIT_DURATION_S":             "Pit Stop (s)",
    "AVG_PIT_S":                  "Avg Pit Stop (s)",
    "PIT_COUNT":                  "Pit Stops",
    "TYRE_COMPOUND":              "Tyre",
    "STINT_LENGTH":               "Stint Laps",
    "WINNER":                     "Winner",
    "WINNER_NAME":                "Winner",
    "WINNER_TEAM":                "Winner Team",
    "YEAR":                       "Season",
}


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={c: COLUMN_LABELS.get(c, c.replace("_", " ").title()) for c in df.columns})


def _run_sql(session, sql: str) -> pd.DataFrame:
    try:
        return session.sql(sql).to_pandas()
    except Exception as e:
        st.error(f"SQL error: {e}")
        return pd.DataFrame()


def _user_wants_chart(prompt: str) -> bool:
    keywords = ["chart", "graph", "plot", "visualis", "visualiz", "bar", "line", "show me"]
    return any(k in prompt.lower() for k in keywords)


def _auto_chart(df: pd.DataFrame):
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    str_cols = df.select_dtypes(include="object").columns.tolist()

    x_col = str_cols[0]
    y_col = numeric_cols[0]

    fig = px.bar(
        df.sort_values(y_col, ascending=False),
        x=x_col, y=y_col,
        template="plotly_dark",
        color_discrete_sequence=["#e8002d"],
    )
    fig.update_layout(
        paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
        margin=dict(l=40, r=20, t=30, b=40),
        height=350,
    )
    st.plotly_chart(fig, use_container_width=True)


def render(session):
    st.markdown("### 🤖 ApexAI")
    st.caption("Ask anything about F1 data — results, lap times, standings, pit stops, tyre strategy and more.")

    # ── Init state ────────────────────────────────────────────────────────────
    if "apexai_messages" not in st.session_state:
        st.session_state["apexai_messages"] = []
    if "apexai_results" not in st.session_state:
        st.session_state["apexai_results"] = {}

    # ── Suggested questions ───────────────────────────────────────────────────
    if not st.session_state["apexai_messages"]:
        st.markdown("**Try asking:**")
        suggestions = [
            "Who won each race in 2025?",
            "What are the current driver championship standings?",
            "Which team had the fastest average pit stop this season?",
            "Who had the fastest lap at the last race?",
        ]
        cols = st.columns(2)
        for i, suggestion in enumerate(suggestions):
            with cols[i % 2]:
                if st.button(suggestion, key=f"sugg_{i}", use_container_width=True):
                    st.session_state["_apexai_prefill"] = suggestion
                    st.rerun()

    # ── Render conversation history ───────────────────────────────────────────
    for idx, msg in enumerate(st.session_state["apexai_messages"]):
        role = msg["role"]
        content = msg["content"]

        if role == "user":
            with st.chat_message("user"):
                st.write(content[0]["text"])

        elif role == "analyst":
            with st.chat_message("assistant"):
                for item in content:
                    if item["type"] == "text":
                        st.write(item["text"])

                if idx in st.session_state["apexai_results"]:
                    cached = st.session_state["apexai_results"][idx]
                    df = cached.get("df")
                    summary = cached.get("summary", "")
                    show_chart = cached.get("show_chart", False)
                    if df is not None and not df.empty:
                        if summary:
                            st.info(summary)
                        if show_chart:
                            _auto_chart(df)
                        st.dataframe(_rename_columns(df), use_container_width=True, hide_index=True)

    # ── Chat input ────────────────────────────────────────────────────────────
    prefill = st.session_state.pop("_apexai_prefill", None)
    prompt = st.chat_input("Ask a question about F1 data...") or prefill

    if prompt:
        user_msg = {"role": "user", "content": [{"type": "text", "text": prompt}]}
        st.session_state["apexai_messages"].append(user_msg)

        with st.chat_message("user"):
            st.write(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    result = _call_analyst(st.session_state["apexai_messages"])
                    analyst_msg = result["message"]
                    st.session_state["apexai_messages"].append(analyst_msg)

                    sql_statement = None
                    for item in analyst_msg["content"]:
                        if item["type"] == "text":
                            st.write(item["text"])
                        elif item["type"] == "sql":
                            sql_statement = item["statement"]
                        elif item["type"] == "suggestions":
                            st.markdown("**Suggested follow-ups:**")
                            for s in item.get("suggestions", []):
                                st.markdown(f"- {s}")

                    if sql_statement:
                        df = _run_sql(session, sql_statement)
                        summary = ""
                        show_chart = _user_wants_chart(prompt)
                        if not df.empty:
                            summary = _call_complete(
                                f"Question: {prompt}\nResults (first 20 rows):\n{df.head(20).to_string(index=False)}"
                            )
                            if summary:
                                st.info(summary)
                            if show_chart:
                                _auto_chart(df)
                            st.dataframe(_rename_columns(df), use_container_width=True, hide_index=True)

                        msg_idx = len(st.session_state["apexai_messages"]) - 1
                        st.session_state["apexai_results"][msg_idx] = {
                            "df": df,
                            "summary": summary,
                            "show_chart": show_chart,
                        }

                except requests.HTTPError as e:
                    st.error(f"Cortex Analyst error: {e.response.status_code} — {e.response.text}")
                except Exception as e:
                    st.error(f"Error: {e}")

    # ── Clear conversation ────────────────────────────────────────────────────
    if st.session_state["apexai_messages"]:
        if st.button("🗑 Clear conversation", key="clear_apexai"):
            st.session_state["apexai_messages"] = []
            st.session_state["apexai_results"] = {}
            st.rerun()
