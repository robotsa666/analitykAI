import io
import pandas as pd
import psycopg2

def load_to_supabase(csv_file, dsn: str, tenant_id: str):
    df = pd.read_csv(io.BytesIO(csv_file.read())) if hasattr(csv_file, "read") else pd.read_csv(csv_file)

    required = {"date","product_name","category","customer_name","region","type","quantity","amount"}
    if not required.issubset(df.columns):
        raise ValueError("Brak wymaganych kolumn w CSV.")

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    dim_product = df[["product_name","category"]].drop_duplicates().copy()
    dim_product["product_id"] = dim_product["product_name"].str.lower().str.replace(r"[^a-z0-9]+","-",regex=True)

    dim_customer = df[["customer_name","region"]].drop_duplicates().copy()
    dim_customer["customer_id"] = dim_customer["customer_name"].str.lower().str.replace(r"[^a-z0-9]+","-",regex=True)

    fact = df.merge(dim_product, on=["product_name","category"], how="left")\
             .merge(dim_customer, on=["customer_name","region"], how="left")
    fact = fact[["date","product_id","customer_id","type","quantity","amount"]]
    fact["tenant_id"] = tenant_id

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            args = list(fact.itertuples(index=False, name=None))
            cur.executemany("""
                INSERT INTO fact_transactions(date,product_id,customer_id,type,quantity,amount,tenant_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, args)
