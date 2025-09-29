# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os

def split_path_to_levels(series: pd.Series, max_levels=50, sep=os.sep):
    levels = {f"Nivel_{i}": [] for i in range(1, max_levels+1)}
    for p in series.astype(str).fillna(""):
        parts = [x for x in p.split(sep) if x not in ("", ".", "..")]
        for i in range(1, max_levels+1):
            levels[f"Nivel_{i}"].append(parts[i-1] if len(parts) >= i else np.nan)
    return pd.DataFrame(levels)

def path_depth_from_levels(df: pd.DataFrame):
    nivel_cols = [c for c in df.columns if c.startswith("Nivel_")]
    if not nivel_cols:
        return pd.Series([np.nan]*len(df), name="Profundidad")
    return df[nivel_cols].notna().sum(axis=1)
