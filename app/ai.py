import os, json, yaml
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

with open(os.path.join(os.path.dirname(__file__), "prompts.json"), "r", encoding="utf-8") as f:
    PROMPTS = json.load(f)

with open(os.path.join(os.path.dirname(__file__), "semantic.yaml"), "r", encoding="utf-8") as f:
    SEM = yaml.safe_load(f)

METRICS = list(SEM["metrics"].keys()) if "metrics" in SEM else []
DIMENSIONS = []
for dname, d in SEM.get("dimensions", {}).items():
    for col in d["columns"]:
        DIMENSIONS.append(f"{dname}.{col}")

def _chat(system: str, user: str, model: str = "gpt-4.1-mini", temperature: float = 0.1) -> str:
    r = client.chat.completions.create(
        model=model,
        messages=[{"role":"system","content":system},{"role":"user","content":user}],
        temperature=temperature
    )
    return r.choices[0].message.content.strip()

def nl2sql(question: str, tenant_id: str) -> str:
    sys = PROMPTS["nl2sql_system"]
    user = PROMPTS["nl2sql_user_template"]\
        .replace("{{metrics_csv}}", ", ".join(METRICS))\
        .replace("{{dimensions_csv}}", ", ".join(DIMENSIONS))\
        .replace("{{question}}", question)\
        .replace("{{tenant_id}}", tenant_id)
    return _chat(sys, user)

def data2chart(columns, sample_rows, preferred="auto") -> dict:
    sys = PROMPTS["data2chart_system"]
    user = PROMPTS["data2chart_user_template"]\
        .replace("{{columns_json}}", json.dumps(columns, ensure_ascii=False))\
        .replace("{{sample_rows_json}}", json.dumps(sample_rows[:10], ensure_ascii=False))\
        .replace("{{preferred}}", preferred)
    txt = _chat(sys, user)
    try:
        return json.loads(txt)
    except json.JSONDecodeError:
        return {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "data": {"values": []},
            "mark": "bar",
            "encoding": {
                "x": {"field": columns[0], "type": "nominal"},
                "y": {"field": columns[1] if len(columns) > 1 else columns[0], "type": "quantitative"}
            }
        }

def data2insight(table_dict: dict, context: str = "") -> str:
    sys = PROMPTS["insight_system"]
    user = PROMPTS["insight_user_template"]\
        .replace("{{table_json}}", json.dumps(table_dict, ensure_ascii=False))\
        .replace("{{context}}", context or "")
    return _chat(sys, user, temperature=0.2)
