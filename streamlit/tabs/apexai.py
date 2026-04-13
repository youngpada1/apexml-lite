import os

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

from utils.jwt_generator import JWTGenerator

SEMANTIC_MODEL = "@APEXML_DB.PROD.CORTEX_STAGE/cortex_model.yaml"
ANALYST_ENDPOINT = "https://{account}.snowflakecomputing.com/api/v2/cortex/analyst/message"
COMPLETE_ENDPOINT = "https://{account}.snowflakecomputing.com/api/v2/cortex/v1/chat/completions"

PREDICTION_KEYWORDS = [
    "predict", "forecast", "will win", "who will", "next race winner",
    "end of season", "projected", "likely to win", "chances", "probability",
]

FORECAST_KEYWORDS = [
    "forecast points", "predict points", "points projection", "points forecast",
    "championship forecast", "championship projection", "end of season standings",
]


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


def _call_complete(system_prompt: str, user_prompt: str, max_tokens: int = 512) -> str:
    resp = requests.post(
        _account_url(COMPLETE_ENDPOINT),
        headers=_headers(),
        json={
            "model": "claude-sonnet-4-5",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_completion_tokens": max_tokens,
            "temperature": 0.4,
        },
        timeout=30,
    )
    if resp.status_code != 200:
        return ""
    return resp.json()["choices"][0]["message"]["content"]


def _run_sql(session, sql: str) -> pd.DataFrame:
    try:
        return session.sql(sql).to_pandas()
    except Exception as e:
        st.error(f"SQL error: {e}")
        return pd.DataFrame()


def _is_prediction(prompt: str) -> bool:
    p = prompt.lower()
    return any(k in p for k in PREDICTION_KEYWORDS)


def _is_forecast(prompt: str) -> bool:
    p = prompt.lower()
    return any(k in p for k in FORECAST_KEYWORDS)


def _handle_prediction(session, prompt: str) -> str:
    """Use COMPLETE with recent race context to generate a prediction."""
    # Fetch last 3 race results + current standings as context
    results_df = _run_sql(session, """
        SELECT r.driver_name, r.team_name, r.finish_position, r.points,
               s.meeting_name, s.country_name, s.session_start_at
        FROM APEXML_DB.PROD.FCT_SESSION_RESULTS r
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON r.session_key = s.session_key
        WHERE s.session_type = 'Race' AND s.session_name = 'Race'
          AND s.session_start_at < CURRENT_TIMESTAMP()
        ORDER BY s.session_start_at DESC
        LIMIT 60
    """)

    standings_df = _run_sql(session, f"""
        SELECT d.full_name, cd.championship_position, cd.points_current, d.team_name
        FROM APEXML_DB.PROD.DIM_CHAMPIONSHIP_DRIVERS cd
        JOIN APEXML_DB.PROD.DIM_DRIVERS d ON cd.session_key = d.session_key AND cd.driver_number = d.driver_number
        WHERE cd.year = YEAR(CURRENT_DATE())
          AND cd.session_key = (SELECT MAX(session_key) FROM APEXML_DB.PROD.DIM_CHAMPIONSHIP_DRIVERS WHERE year = YEAR(CURRENT_DATE()))
        ORDER BY cd.championship_position
        LIMIT 10
    """)

    next_race_df = _run_sql(session, """
        SELECT meeting_name, country_name, circuit_short_name, session_start_at
        FROM APEXML_DB.PROD.DIM_SESSIONS
        WHERE session_type = 'Race' AND session_name = 'Race'
          AND session_start_at > CURRENT_TIMESTAMP()
        ORDER BY session_start_at
        LIMIT 1
    """)

    context = ""
    if not next_race_df.empty:
        nr = next_race_df.iloc[0]
        context += f"Next race: {nr['MEETING_NAME']} at {nr['CIRCUIT_SHORT_NAME']}, {nr['COUNTRY_NAME']}\n\n"

    if not standings_df.empty:
        context += "Current championship standings (top 10):\n"
        context += standings_df.to_string(index=False) + "\n\n"

    if not results_df.empty:
        context += "Recent race results (last 3 races):\n"
        context += results_df.to_string(index=False) + "\n"

    return _call_complete(
        system_prompt=(
            "You are an expert F1 analyst. Based on the data provided, answer the user's prediction question. "
            "Be analytical and specific. Reference actual data points. Keep your answer to 3-5 sentences. "
            "Be clear this is a data-driven prediction, not a guaranteed outcome."
        ),
        user_prompt=f"Question: {prompt}\n\nData:\n{context}",
        max_tokens=512,
    )


def _handle_forecast(session, prompt: str) -> tuple[str, pd.DataFrame]:
    """Call the pre-trained SNOWFLAKE.ML.FORECAST model to project championship points."""
    # Get remaining races this season
    remaining_df = _run_sql(session, """
        SELECT COUNT(*) AS remaining_races
        FROM APEXML_DB.PROD.DIM_SESSIONS
        WHERE session_type = 'Race' AND session_name = 'Race'
          AND year = YEAR(CURRENT_DATE())
          AND session_start_at > CURRENT_TIMESTAMP()
    """)
    remaining = int(remaining_df.iloc[0]["REMAINING_RACES"]) if not remaining_df.empty else 0

    if remaining == 0:
        return "No remaining races this season to forecast.", pd.DataFrame()

    # Call the pre-trained ML forecast model
    forecast_df = _run_sql(session, f"""
        WITH raw_forecast AS (
            SELECT * FROM TABLE(
                APEXML_DB.PROD.FORECAST_MODEL_CORTEX!FORECAST(FORECASTING_PERIODS => {remaining})
            )
        )
        SELECT
            series_id                          AS driver_name,
            MAX(forecast)                      AS projected_points,
            MAX(upper_bound)                   AS upper_bound,
            MAX(lower_bound)                   AS lower_bound
        FROM raw_forecast
        GROUP BY series_id
        ORDER BY projected_points DESC
        LIMIT 10
    """)

    if forecast_df.empty:
        return (
            "Forecast model returned no results. It may need to be initialised — "
            "please run the setup SQL in the Snowflake UI first.",
            pd.DataFrame(),
        )

    # COMPLETE narrative summary
    narrative = _call_complete(
        system_prompt=(
            "You are an F1 championship analyst. Based on the ML forecast data provided, "
            "summarise the projected championship outcome. Reference the top 3 drivers by name and projected points. "
            "Keep it to 3-4 sentences. Be clear this is a statistical projection."
        ),
        user_prompt=f"Question: {prompt}\n\nProjected final standings:\n{forecast_df.to_string(index=False)}",
        max_tokens=400,
    )

    return narrative, forecast_df


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


def _user_wants_chart(prompt: str) -> bool:
    keywords = ["chart", "graph", "plot", "visualis", "visualiz", "bar", "line", "show me"]
    return any(k in prompt.lower() for k in keywords)


def _auto_chart(df: pd.DataFrame):
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    str_cols = df.select_dtypes(include="object").columns.tolist()
    if not str_cols or not numeric_cols:
        return
    fig = px.bar(
        df.sort_values(numeric_cols[0], ascending=False),
        x=str_cols[0], y=numeric_cols[0],
        template="plotly_dark",
        color_discrete_sequence=["#e8002d"],
    )
    fig.update_layout(
        paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
        margin=dict(l=40, r=20, t=30, b=40),
        height=350,
    )
    st.plotly_chart(fig, use_container_width=True)


def _append_prediction_message(text: str):
    msg = {"role": "analyst", "content": [{"type": "text", "text": text}]}
    st.session_state["apexai_messages"].append(msg)
    return msg


def render(session):
    st.markdown("### 🤖 ApexAI")
    st.caption("Ask about F1 results, standings, lap times, strategy — or request **predictions** and **championship forecasts**.")

    # ── Init state ────────────────────────────────────────────────────────────
    if "apexai_messages" not in st.session_state:
        st.session_state["apexai_messages"] = []
    if "apexai_results" not in st.session_state:
        st.session_state["apexai_results"] = {}

    # ── Suggested questions ───────────────────────────────────────────────────
    if not st.session_state["apexai_messages"]:
        st.markdown("**Try asking:**")
        suggestions = [
            "Who won each race this year?",
            "What are the current driver championship standings?",
            "Who will win the next race?",
            "Forecast the championship standings at the end of the season",
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
    prompt = st.chat_input("Ask a question, request a prediction or forecast...") or prefill

    if prompt:
        user_msg = {"role": "user", "content": [{"type": "text", "text": prompt}]}
        st.session_state["apexai_messages"].append(user_msg)

        with st.chat_message("user"):
            st.write(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    # ── Route: forecast ───────────────────────────────────────
                    if _is_forecast(prompt):
                        narrative, df = _handle_forecast(session, prompt)
                        st.write(narrative)
                        analyst_msg = _append_prediction_message(narrative)
                        msg_idx = len(st.session_state["apexai_messages"]) - 1
                        show_chart = _user_wants_chart(prompt)
                        if not df.empty:
                            if show_chart:
                                _auto_chart(df)
                            st.dataframe(_rename_columns(df), use_container_width=True, hide_index=True)
                        st.session_state["apexai_results"][msg_idx] = {
                            "df": df, "summary": "", "show_chart": show_chart,
                        }

                    # ── Route: prediction ─────────────────────────────────────
                    elif _is_prediction(prompt):
                        prediction = _handle_prediction(session, prompt)
                        st.write(prediction)
                        _append_prediction_message(prediction)

                    # ── Route: data question → Cortex Analyst ─────────────────
                    else:
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
                                    system_prompt="You are an F1 data analyst. Summarise the following query results concisely in 2-3 sentences. Focus on the key insight, not the data format.",
                                    user_prompt=f"Question: {prompt}\nResults (first 20 rows):\n{df.head(20).to_string(index=False)}",
                                    max_tokens=256,
                                )
                                if summary:
                                    st.info(summary)
                                if show_chart:
                                    _auto_chart(df)
                                st.dataframe(_rename_columns(df), use_container_width=True, hide_index=True)

                            msg_idx = len(st.session_state["apexai_messages"]) - 1
                            st.session_state["apexai_results"][msg_idx] = {
                                "df": df, "summary": summary, "show_chart": show_chart,
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
