# -*- coding: utf-8 -*-
import pandas as pd, numpy as np
def norm_ext(ext):
    if ext is None or (isinstance(ext, float) and np.isnan(ext)): return np.nan
    e = str(ext).strip().lstrip(".").lower()
    return e if e else np.nan
def mime_ext_mismatch(df):
    if "MimeType" not in df.columns or "Extension" not in df.columns: return pd.DataFrame()
    t = df[["Nombre","Extension","MimeType","TamanoBytes","RutaCompleta","RutaRelativa"]].copy()
    t["ExtensionNorm"] = t["Extension"].map(norm_ext)
    def ok(row):
        ext = str(row["ExtensionNorm"] or "").lower()
        mime = str(row["MimeType"] or "").lower()
        if not ext or not mime: return False
        return ext in mime
    t["Consistente"] = t.apply(ok, axis=1)
    return t[t["Consistente"]==False]
