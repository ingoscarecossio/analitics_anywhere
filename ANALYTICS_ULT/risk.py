# -*- coding: utf-8 -*-
import pandas as pd, numpy as np
from .path_utils import path_depth_from_levels
from .security import world_writable, world_readable

DEFAULT_POLICIES = {
    "stale_days": 365, "long_path": 255, "deep_levels": 20, "big_bytes": 2*1024**3,
    "bad_name_patterns": ["copia","copy","tmp","backup","old","viejo"],
    "weights": {"big_file":3,"duplicate_hash":4,"world_writable":4,"world_readable":1,"hidden":1,"long_path":2,"deep_levels":2,"stale":2,"bad_name":1},
    "risk_bins": [-1,1,3,6,100], "risk_labels":["Bajo","Medio","Alto","Crítico"]
}

def risk_scoring(df, policies=None):
    if policies is None: policies = DEFAULT_POLICIES
    t = df.copy()
    t["TamanoBytes"] = pd.to_numeric(t.get("TamanoBytes"), errors="coerce")
    for c in ["FechaCreacion","FechaModificacion","FechaAcceso"]:
        if c in t.columns:
            t[c] = pd.to_datetime(t[c], errors="coerce")
            try: t[c] = t[c].dt.tz_localize(None)
            except Exception: pass
    t["Profundidad"] = path_depth_from_levels(t) if "Nivel_1" in t.columns or "CarpetaPadre" in t.columns else np.nan

    w = policies.get("weights", {})
    now = pd.Timestamp.now().tz_localize(None)
    stale_days = policies.get("stale_days", 365)
    long_path = policies.get("long_path", 255)
    deep_levels = policies.get("deep_levels", 20)
    big_bytes = policies.get("big_bytes", 2*1024**3)
    patterns = policies.get("bad_name_patterns", ["copia","copy","tmp","backup","old","viejo"])

    # precalculo de duplicados por hash si existe
    dup_mask = pd.Series([False]*len(t))
    if "Hash" in t.columns:
        counts = t["Hash"].value_counts()
        dup_mask = t["Hash"].map(lambda h: counts.get(h,0)>1)

    pts = []
    reasons = []
    for i, row in t.iterrows():
        p = 0; why = []
        if not pd.isna(row.get("TamanoBytes")) and row["TamanoBytes"] >= big_bytes: p += w.get("big_file",3); why.append("big_file")
        if "Hash" in t.columns and bool(dup_mask.iloc[i]): p += w.get("duplicate_hash",4); why.append("duplicate_hash")
        if "PermOctal" in t.columns:
            if world_writable(row.get("PermOctal")): p += w.get("world_writable",4); why.append("world_writable")
            elif world_readable(row.get("PermOctal")): p += w.get("world_readable",1); why.append("world_readable")
        if "Oculto" in t.columns and row.get("Oculto") is True: p += w.get("hidden",1); why.append("hidden")
        ruta = row.get("RutaCompleta") or row.get("RutaRelativa") or ""
        if isinstance(ruta, str) and len(ruta) > long_path: p += w.get("long_path",2); why.append("long_path")
        if not pd.isna(row.get("Profundidad")) and row["Profundidad"] > deep_levels: p += w.get("deep_levels",2); why.append("deep_levels")
        base_date = row.get("FechaAcceso") or row.get("FechaModificacion") or row.get("FechaCreacion")
        if pd.notna(base_date) and (now - base_date).days > stale_days: p += w.get("stale",2); why.append("stale")
        name = str(row.get("Nombre") or "").lower()
        if any(k in name for k in patterns): p += w.get("bad_name",1); why.append("bad_name")
        pts.append(p); reasons.append(", ".join(why))
    t["RiskScore"] = pts
    t["RiskWhy"] = reasons
    bins = policies.get("risk_bins", [-1,1,3,6,100])
    labels = policies.get("risk_labels", ["Bajo","Medio","Alto","Crítico"])
    t["RiskBand"] = pd.cut(t["RiskScore"], bins=bins, labels=labels)
    return t.sort_values(["RiskScore","TamanoBytes"], ascending=[False, False])
