import re
import sqlglot
from sqlglot.errors import ParseError

class GuardError(Exception):
    pass

BANNED = re.compile(r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|COPY|GRANT|REVOKE|MERGE)\b", re.I)

def guard_sql(sql: str) -> str:
    s = sql.strip()
    if ";" in s.rstrip(";"):
        raise GuardError("Wiele statementów zabronione")
    try:
        sqlglot.parse_one(s, read="postgres")
    except ParseError:
        raise GuardError("Niepoprawny SQL")
    if BANNED.search(s):
        raise GuardError("Niedozwolona operacja w SQL")
    if "fact_transactions" not in s.lower():
        raise GuardError("Zapytanie musi korzystać z fact_transactions")
    if "tenant_id" not in s.lower():
        raise GuardError("Brak filtra tenant_id = :tenant")
    if "group by" not in s.lower() and "limit" not in s.lower():
        s += " LIMIT 500"
    return s
