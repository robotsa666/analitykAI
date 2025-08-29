import os, json
import pandas as pd
import psycopg2
import streamlit as st

# --- Sekrety / env ---
OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY"))
DB_DSN = st.secrets.get("DB_DSN", os.getenv("DB_DSN"))
TENANT_ID = st.secrets.get("TENANT_ID", os.getenv("TENANT_ID", "00000000-0000-0000-0000-000000000000"))

if OPENAI_API_KEY and not os.getenv("OPENAI_API_KEY"):
    os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

from app.ai import nl2sql, data2chart, data2insight
from app.sql_guard import guard_sql, GuardError

st.set_page_config(page_title="AI BI – demo", layout="wide")

if not OPENAI_API_KEY or not DB_DSN:
    st.error("Brak OPENAI_API_KEY lub DB_DSN w sekretach/zmiennych środowiskowych.")
    st.stop()

st.title("AI Analityk dla MŚP – demo")
st.caption("Wgraj dane → zapytaj po polsku → SQL → wykres + wnioski.")

# ---------- Upload ----------
with st.expander("Wgraj dane (CSV wg app/sample_data.csv)"):
    up = st.file_uploader("Plik CSV", type=["csv"])
    if up is not None:
        from app.etl import load_to_supabase
        try:
            load_to_supabase(up, DB_DSN, TENANT_ID)
            st.success("Dane załadowane do Supabase ✅")
        except Exception as e:
            st.error(f"Błąd ETL: {e}")

# ---------- Query ----------
question = st.text_input("Twoje pytanie", "Pokaż miesięczne przychody i marżę brutto % w 1 kwartale 2025 wg kategorii produktu.")
run = st.button("Analizuj")

def run_sql(sql: str, params: dict):
    with psycopg2.connect(DB_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            columns = [d[0] for d in cur.description]
            return pd.DataFrame(rows, columns=columns)

if run and question.strip():
    try:
        with st.spinner("Generuję SQL…"):
            sql = nl2sql(question, TENANT_ID)
            sql = guard_sql(sql)
            st.code(sql, language="sql")

        with st.spinner("Pobieram dane…"):
            df = run_sql(sql, {"tenant": TENANT_ID})
            if df.empty:
                st.warning("Brak danych dla zapytania.")
                st.stop()

        with st.spinner("Generuję wykres…"):
            columns = list(df.columns)
            sample_rows = df.head(10).values.tolist()
            spec = data2chart(columns, sample_rows, preferred="auto")
            spec["data"] = {"values": df.to_dict(orient="records")}
            st.vega_lite_chart(spec, use_container_width=True)

        with st.spinner("Interpretuję wyniki…"):
            table_dict = {"columns": columns, "rows": df.values.tolist()}
            insight = data2insight(table_dict, context="Zapytanie użytkownika")
            st.success("Wnioski AI")
            st.write(insight)

    except GuardError as ge:
        st.error(f"Zapytanie odrzucone przez strażnika SQL: {ge}")
    except Exception as e:
        st.error(f"Nieoczekiwany błąd: {e}")

st.markdown("---\nMVP • NL→SQL • Vega-Lite • Supabase Postgres • OpenAI")
