# -*- coding: utf-8 -*-
import pandas as pd, numpy as np

def simulate_dedupe(df: pd.DataFrame, by="CarpetaPadre", strategy="keep-largest"):
    """
    Simula deduplicación por Hash dentro de cada grupo `by`.
    strategy: keep-largest | keep-earliest | keep-latest
    Retorna: (plan, ahorro_total_bytes)
    """
    if "Hash" not in df.columns:
        return pd.DataFrame(), 0.0
    t = df.copy()
    t["TamanoBytes"] = pd.to_numeric(t.get("TamanoBytes"), errors="coerce")
    # fecha criterio
    for c in ["FechaCreacion","FechaModificacion","FechaAcceso"]:
        if c in t.columns:
            t[c] = pd.to_datetime(t[c], errors="coerce")
    plans = []
    ahorro = 0.0
    group_cols = [by] if by in t.columns else [c for c in ["CarpetaPadre","Propietario","Extension","Raiz"] if c in t.columns][:1]
    if not group_cols: group_cols = ["Hash"]  # fallback
    for (grp, gdf) in t.groupby(group_cols + ["Hash"], dropna=False):
        if len(gdf) <= 1: 
            continue
        if strategy=="keep-largest":
            keep = gdf.sort_values("TamanoBytes", ascending=False).head(1)
        elif strategy=="keep-earliest":
            datecol = "FechaCreacion" if "FechaCreacion" in gdf.columns else ("FechaModificacion" if "FechaModificacion" in gdf.columns else None)
            keep = gdf.sort_values(datecol, ascending=True).head(1) if datecol else gdf.head(1)
        elif strategy=="keep-latest":
            datecol = "FechaModificacion" if "FechaModificacion" in gdf.columns else ("FechaAcceso" if "FechaAcceso" in gdf.columns else None)
            keep = gdf.sort_values(datecol, ascending=False).head(1) if datecol else gdf.head(1)
        else:
            keep = gdf.head(1)
        drop = gdf.loc[~gdf.index.isin(keep.index)]
        ahorro += pd.to_numeric(drop["TamanoBytes"], errors="coerce").sum()
        plan_block = drop.copy()
        plan_block["Action"] = "DeleteDuplicate"
        plan_block["Keep_Ref"] = keep.iloc[0].get("RutaCompleta") or keep.iloc[0].get("RutaRelativa")
        plans.append(plan_block)
    plan = pd.concat(plans, axis=0, ignore_index=True) if plans else pd.DataFrame()
    return plan, float(ahorro)
