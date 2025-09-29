# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
def validate_sizes(df: pd.DataFrame):
    if "TamanoBytes" not in df.columns: return pd.DataFrame()
    return df[pd.to_numeric(df["TamanoBytes"], errors="coerce") < 0].copy()
def validate_dates(df: pd.DataFrame):
    out = []
    cols = [c for c in ["FechaCreacion","FechaModificacion","FechaAcceso"] if c in df.columns]
    if not cols: return pd.DataFrame()
    now = pd.Timestamp.now().tz_localize(None)
    t = df.copy()
    for c in cols:
        t[c] = pd.to_datetime(t[c], errors="coerce")
        try: t[c] = t[c].dt.tz_localize(None)
        except Exception: pass
    for c in cols:
        bad = t[t[c] > now].copy(); 
        if not bad.empty: bad["Regla"] = f"{c} en el futuro"; out.append(bad)
    if set(["FechaCreacion","FechaAcceso"]).issubset(t.columns):
        bad = t[t["FechaAcceso"] < t["FechaCreacion"]].copy()
        if not bad.empty: bad["Regla"] = "Acceso antes de Creación"; out.append(bad)
    if set(["FechaCreacion","FechaModificacion"]).issubset(t.columns):
        bad = t[t["FechaModificacion"] < t["FechaCreacion"]].copy()
        if not bad.empty: bad["Regla"] = "Modificación antes de Creación"; out.append(bad)
    return pd.concat(out, axis=0, ignore_index=True) if out else pd.DataFrame()
def anomalies_size_iqr(df: pd.DataFrame):
    if "TamanoBytes" not in df.columns: return pd.DataFrame()
    s = pd.to_numeric(df["TamanoBytes"], errors="coerce").dropna()
    if s.empty: return pd.DataFrame()
    q1, q3 = s.quantile(0.25), s.quantile(0.75); iqr = q3 - q1
    upper = q3 + 1.5*iqr; lower = max(0, q1 - 1.5*iqr)
    mask = (pd.to_numeric(df["TamanoBytes"], errors="coerce") > upper) | (pd.to_numeric(df["TamanoBytes"], errors="coerce") < lower)
    return df[mask].copy()
