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
    """Use Cortex COMPLETE with rich PROD context to forecast championship outcome."""
    # Current standings
    standings_df = _run_sql(session, """
        SELECT d.full_name, d.team_name, cd.championship_position, cd.points_current
        FROM APEXML_DB.PROD.DIM_CHAMPIONSHIP_DRIVERS cd
        JOIN APEXML_DB.PROD.DIM_DRIVERS d ON cd.session_key = d.session_key AND cd.driver_number = d.driver_number
        WHERE cd.year = YEAR(CURRENT_DATE())
          AND cd.session_key = (SELECT MAX(session_key) FROM APEXML_DB.PROD.DIM_CHAMPIONSHIP_DRIVERS WHERE year = YEAR(CURRENT_DATE()))
        ORDER BY cd.championship_position
        LIMIT 10
    """)

    # Recent race results (last 5 races)
    results_df = _run_sql(session, """
        SELECT r.driver_name, r.team_name, r.finish_position, r.points, r.grid_position,
               s.meeting_name, s.country_name
        FROM APEXML_DB.PROD.FCT_SESSION_RESULTS r
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON r.session_key = s.session_key
        WHERE s.session_type = 'Race' AND s.session_name = 'Race'
          AND s.year = YEAR(CURRENT_DATE())
          AND s.session_start_at < CURRENT_TIMESTAMP()
        ORDER BY s.session_start_at DESC
        LIMIT 100
    """)

    # Average lap times per driver this season
    laps_df = _run_sql(session, """
        SELECT driver_name, team_name,
               ROUND(AVG(lap_duration_s), 3) AS avg_lap_s,
               ROUND(MIN(lap_duration_s), 3) AS best_lap_s,
               COUNT(*) AS total_laps
        FROM APEXML_DB.PROD.FCT_LAPS
        WHERE year = YEAR(CURRENT_DATE())
          AND lap_duration_s IS NOT NULL
        GROUP BY driver_name, team_name
        ORDER BY avg_lap_s
        LIMIT 20
    """)

    # Pit stop performance per driver
    pits_df = _run_sql(session, """
        SELECT driver_name, team_name,
               ROUND(AVG(pit_duration_s), 2) AS avg_pit_s,
               COUNT(*) AS total_stops
        FROM APEXML_DB.PROD.FCT_PIT_STOPS
        WHERE year = YEAR(CURRENT_DATE())
          AND pit_duration_s IS NOT NULL
        GROUP BY driver_name, team_name
        ORDER BY avg_pit_s
        LIMIT 20
    """)

    # Tyre strategy — most used compounds per driver
    stints_df = _run_sql(session, """
        SELECT driver_name, tyre_compound, COUNT(*) AS stints
        FROM APEXML_DB.PROD.FCT_STINTS
        WHERE year = YEAR(CURRENT_DATE())
        GROUP BY driver_name, tyre_compound
        ORDER BY driver_name, stints DESC
        LIMIT 40
    """)

    # Remaining races
    remaining_df = _run_sql(session, """
        SELECT COUNT(*) AS remaining_races
        FROM APEXML_DB.PROD.DIM_SESSIONS
        WHERE session_type = 'Race' AND session_name = 'Race'
          AND year = YEAR(CURRENT_DATE())
          AND session_start_at > CURRENT_TIMESTAMP()
    """)
    remaining = int(remaining_df.iloc[0]["REMAINING_RACES"]) if not remaining_df.empty else 0

    if remaining == 0:
        return "No remaining races this season to forecast.", standings_df

    # Build context
    context = f"There are {remaining} races remaining this season.\n\n"
    if not standings_df.empty:
        context += "Current championship standings:\n" + standings_df.to_string(index=False) + "\n\n"
    if not results_df.empty:
        context += "Recent race results (last 5 races):\n" + results_df.to_string(index=False) + "\n\n"
    if not laps_df.empty:
        context += "Average lap times per driver:\n" + laps_df.to_string(index=False) + "\n\n"
    if not pits_df.empty:
        context += "Pit stop performance per driver:\n" + pits_df.to_string(index=False) + "\n\n"
    if not stints_df.empty:
        context += "Tyre strategy per driver:\n" + stints_df.to_string(index=False) + "\n"

    narrative = _call_complete(
        system_prompt=(
            "You are an expert F1 championship analyst. Using all the data provided — standings, "
            "recent results, lap times, pit stop performance and tyre strategy — project the likely "
            "championship outcome for the remaining races. Estimate projected final points for the top 5 "
            "drivers. Be specific with numbers and reference actual data points. Keep it to 4-6 sentences."
        ),
        user_prompt=f"Question: {prompt}\n\nData:\n{context}",
        max_tokens=600,
    )

    return narrative, standings_df


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


def _smart_chart(df: pd.DataFrame, prompt: str):
    """Ask COMPLETE which columns and chart type to use, then render."""
    import json
    cols_info = {c: str(df[c].dtype) for c in df.columns}
    sample = df.head(5).to_string(index=False)

    decision = _call_complete(
        system_prompt=(
            "You are a data visualisation expert. Respond with ONLY a single line of valid JSON. "
            "No explanation, no markdown, no text before or after. "
            'Format: {"chart": "bar"|"line"|"scatter", "x": "col_name", "y": "col_name", "color": "col_name"|null}. '
            "Pick the most meaningful numeric column for y (not IDs, keys, or sequence numbers). "
            "Use color for categorical grouping if useful."
        ),
        user_prompt=f"User asked: {prompt}\nColumns: {cols_info}\nSample:\n{sample}",
        max_tokens=100,
    )

    try:
        spec = json.loads(decision)
        x, y, color = spec.get("x"), spec.get("y"), spec.get("color")
        chart_type = spec.get("chart", "bar")
        if x not in df.columns or y not in df.columns:
            return
        if color and color not in df.columns:
            color = None

        layout = dict(
            paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
            margin=dict(l=40, r=20, t=30, b=40),
            height=350, showlegend=False,
        )

        if chart_type == "line":
            fig = px.line(df, x=x, y=y, color=color, template="plotly_dark")
        elif chart_type == "scatter":
            fig = px.scatter(df, x=x, y=y, color=color, template="plotly_dark")
        else:
            fig = px.bar(df, x=x, y=y, color=color, barmode="group", template="plotly_dark")

        fig.update_layout(**layout)
        st.plotly_chart(fig, use_container_width=True)
    except Exception:
        return



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
                    cached_prompt = cached.get("prompt", "")
                    if df is not None and not df.empty:
                        if summary:
                            st.info(summary)
                        if show_chart:
                            _smart_chart(df, cached_prompt)
                        st.dataframe(_rename_columns(df), use_container_width=True, hide_index=True)

    # ── Chat input ────────────────────────────────────────────────────────────
    prefill = st.session_state.pop("_apexai_prefill", None)
    prompt = st.chat_input("Ask a question, request a prediction or forecast...") or prefill

    if prompt:
        user_msg = {"role": "user", "content": [{"type": "text", "text": prompt}], "_route": "analyst"}
        st.session_state["apexai_messages"].append(user_msg)

        with st.chat_message("user"):
            st.write(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    # ── Route: forecast ───────────────────────────────────────
                    if _is_forecast(prompt):
                        st.session_state["apexai_messages"][-1]["_route"] = "complete"
                        narrative, df = _handle_forecast(session, prompt)
                        st.write(narrative)
                        show_chart = _user_wants_chart(prompt)
                        if not df.empty:
                            if show_chart:
                                _smart_chart(df, prompt)
                            st.dataframe(_rename_columns(df), use_container_width=True, hide_index=True)

                    # ── Route: prediction ─────────────────────────────────────
                    elif _is_prediction(prompt):
                        st.session_state["apexai_messages"][-1]["_route"] = "complete"
                        prediction = _handle_prediction(session, prompt)
                        st.write(prediction)

                    # ── Route: data question → Cortex Analyst ─────────────────
                    else:
                        analyst_history = [
                            {k: v for k, v in m.items() if k != "_route"}
                            for m in st.session_state["apexai_messages"]
                            if m.get("_route", "analyst") == "analyst"
                        ]
                        result = _call_analyst(analyst_history)
                        analyst_msg = result["message"]
                        analyst_msg["_route"] = "analyst"
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
                                    _smart_chart(df, prompt)
                                st.dataframe(_rename_columns(df), use_container_width=True, hide_index=True)

                            msg_idx = len(st.session_state["apexai_messages"]) - 1
                            st.session_state["apexai_results"][msg_idx] = {
                                "df": df, "summary": summary, "show_chart": show_chart, "prompt": prompt,
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
