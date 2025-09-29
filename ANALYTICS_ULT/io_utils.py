# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np

SUPPORTED_EXCEL = {".xlsx", ".xlsm", ".xls", ".xlsb"}
SUPPORTED_CSV = {".csv", ".txt"}

def load_table(path: str, sheet_name=None, sep=",", encoding="utf-8", nrows=None) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"No existe el archivo: {path}")
    ext = os.path.splitext(path)[1].lower()
    if ext in SUPPORTED_EXCEL:
        _df = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
        if isinstance(_df, dict):
            first_key = list(_df.keys())[0]
            return _df[first_key]
        return _df
    elif ext in SUPPORTED_CSV:
        try:
            return pd.read_csv(path, sep=sep, encoding=encoding, nrows=nrows)
        except Exception:
            for sep_try in [";", ",", "\t", "|"]:
                for enc_try in ["utf-8", "latin-1", "cp1252"]:
                    try:
                        return pd.read_csv(path, sep=sep_try, encoding=enc_try, nrows=nrows)
                    except Exception:
                        continue
            raise
    else:
        raise ValueError(f"Extensión no soportada: {ext}")

def coerce_booleans(df: pd.DataFrame, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.lower().map({
                "true":"True","false":"False","1":"True","0":"False","sí":"True","si":"True","no":"False"
            }).fillna(df[c])
            df[c] = df[c].map(lambda x: True if str(x).lower() in {"true","1"} else (False if str(x).lower() in {"false","0"} else np.nan))
    return df

def coerce_datetimes(df: pd.DataFrame, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
            try: df[c] = df[c].dt.tz_localize(None)
            except Exception: pass
    return df

def coerce_numeric(df: pd.DataFrame, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def human_bytes(num, suffix="B"):
    if pd.isna(num):
        return "NA"
    for unit in ["","K","M","G","T","P","E","Z"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Y{suffix}"
