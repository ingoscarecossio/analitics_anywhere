# -*- coding: utf-8 -*-
"""
Analizadores principales para Anywhere Analytics ULTIMATE.

Incluye:
- KPIs generales (overview_metrics)
- Top por tamaño
- Calidad de datos (missingness)
- Frecuencias (freq_table)
- Duplicados por Hash/PseudoHash (duplicates_by_hash)
- Series temporales por periodo (timeline_counts)
- Agregaciones por categoría/carpeta (agg_by / agg_by_folder)
- Explicabilidad de riesgo (explain_risk_row)
- Buckets de tamaño (size_buckets)  ← con hotfix pandas 2.x
- KPIs avanzados (kpi_advanced)
"""

import pandas as pd
import numpy as np

from .io_utils import human_bytes
from .path_utils import path_depth_from_levels  # por si se requiere
from .security import world_writable, world_readable


def overview_metrics(df: pd.DataFrame):
    met = {"filas": len(df), "columnas": df.shape[1]}
    if "TamanoBytes" in df.columns:
        s = pd.to_numeric(df["TamanoBytes"], errors="coerce")
        total = s.sum()
        met.update({
            "tamano_total_bytes": float(total),
            "tamano_total_humano": human_bytes(total),
            "tamano_promedio": float(s.mean()),
            "archivos_cero_bytes": int((s == 0).sum())
        })
    if "Extension" in df.columns:
        met["extensiones_unicas"] = int(df["Extension"].astype(str).nunique(dropna=True))
    if "Hash" in df.columns:
        met["hash_nulos"] = int(df["Hash"].isna().sum())
    if "Oculto" in df.columns:
        met["porc_ocultos"] = float((df["Oculto"] == True).mean() * 100)  # noqa: E712
    if "SoloLectura" in df.columns:
        met["porc_solo_lectura"] = float((df["SoloLectura"] == True).mean() * 100)  # noqa: E712
    return met


def top_n_by_size(df: pd.DataFrame, n: int = 50) -> pd.DataFrame:
    if "TamanoBytes" not in df.columns:
        return pd.DataFrame()
    t = df.copy()
    t["TamanoBytes"] = pd.to_numeric(t["TamanoBytes"], errors="coerce")
    return t.sort_values("TamanoBytes", ascending=False).head(n)


def missingness(df: pd.DataFrame) -> pd.DataFrame:
    m = df.isna().sum().to_frame("faltantes")
    if len(df) == 0:
        m["porcentaje"] = 0.0
    else:
        m["porcentaje"] = (m["faltantes"] / len(df) * 100).round(2)
    return m.sort_values("porcentaje", ascending=False)


def freq_table(df: pd.DataFrame, col: str, n: int = 30) -> pd.DataFrame:
    if col not in df.columns:
        return pd.DataFrame()
    s = df[col].astype("string")
    tab = s.value_counts(dropna=False).to_frame("conteo")
    if len(s) == 0:
        tab["porcentaje"] = 0.0
    else:
        tab["porcentaje"] = (tab["conteo"] / len(s) * 100).round(2)
    return tab.head(n).reset_index(names=col)


def duplicates_by_hash(df: pd.DataFrame):
    """
    Devuelve (tabla_de_duplicados, espacio_potencial_recuperable_en_bytes)
    Si no hay 'Hash', usa un PseudoHash con (Nombre|TamanoBytes).
    """
    use_col = None
    t = df.copy()

    if "Hash" in t.columns:
        use_col = "Hash"
    elif set(["Nombre", "TamanoBytes"]).issubset(t.columns):
        t["__PseudoHash__"] = t["Nombre"].astype(str) + "|" + t["TamanoBytes"].astype(str)
        use_col = "__PseudoHash__"
    else:
        return pd.DataFrame(), 0.0

    g = t.groupby(use_col, dropna=True, as_index=False).agg(
        conteo=("Nombre", "size"),
        tam_total=("TamanoBytes", lambda x: pd.to_numeric(x, errors="coerce").sum())
    )
    dup = g[g["conteo"] > 1].sort_values(["conteo", "tam_total"], ascending=[False, False])

    tam_uniq = t.dropna(subset=[use_col]).drop_duplicates(subset=[use_col])["TamanoBytes"]
    espacio_potencial = float(
        pd.to_numeric(t["TamanoBytes"], errors="coerce").sum() - pd.to_numeric(tam_uniq, errors="coerce").sum()
    )
    return dup, max(0.0, espacio_potencial)


def timeline_counts(df: pd.DataFrame, date_col: str, freq: str = "M") -> pd.DataFrame:
    if date_col not in df.columns:
        return pd.DataFrame()
    s = pd.to_datetime(df[date_col], errors="coerce")
    # normalizar tz para Excel/plots
    try:
        s = s.dt.tz_localize(None)
    except Exception:
        pass
    gr = s.dt.to_period(freq).value_counts().sort_index()
    out = gr.rename_axis("periodo").reset_index(name="conteo")
    out["periodo"] = out["periodo"].astype(str)
    return out


def agg_by(df: pd.DataFrame, base_col: str, top: int = 50) -> pd.DataFrame:
    if base_col not in df.columns:
        return pd.DataFrame()
    t = df.copy()
    t["TamanoBytes"] = pd.to_numeric(t.get("TamanoBytes"), errors="coerce")
    g = t.groupby(base_col, dropna=False).agg(
        archivos=("Nombre", "size"),
        tam_total=("TamanoBytes", "sum")
    ).reset_index().rename(columns={base_col: "Categoria"})
    g["tam_total_humano"] = g["tam_total"].map(human_bytes)
    return g.sort_values(["tam_total", "archivos"], ascending=[False, False]).head(top)


def agg_by_folder(df: pd.DataFrame, top: int = 50) -> pd.DataFrame:
    base_col = "CarpetaPadre" if "CarpetaPadre" in df.columns else None
    if not base_col:
        niveles = sorted(
            [c for c in df.columns if c.startswith("Nivel_")],
            key=lambda x: int(x.split("_")[1])
        )
        base_col = niveles[-1] if niveles else None
    if not base_col:
        return pd.DataFrame()
    return agg_by(df, base_col, top=top)


def explain_risk_row(row: pd.Series, policies: dict) -> str:
    """
    Genera un texto breve con las razones más comunes del puntaje de riesgo.
    (Complementa al módulo risk.py, útil para tooltips o auditoría rápida).
    """
    reasons = []
    w = policies.get("weights", {})

    # Tamaño grande
    big_thr = policies.get("big_bytes", 2 * 1024 ** 3)
    if not pd.isna(row.get("TamanoBytes")) and row["TamanoBytes"] >= big_thr:
        reasons.append(f"tamaño≥{big_thr} (+{w.get('big_file', 3)})")

    # Permisos
    rwx = str(row.get("Perm_RWX") or "")
    if rwx.endswith("w"):
        reasons.append(f"world-writable (+{w.get('world_writable', 4)})")
    elif rwx.endswith("r"):
        reasons.append(f"world-readable (+{w.get('world_readable', 1)})")

    # Oculto
    if row.get("Oculto") is True:
        reasons.append(f"oculto (+{w.get('hidden', 1)})")

    # Longitud de ruta / profundidad
    if row.get("LongRuta", 0) > policies.get("long_path", 255):
        reasons.append(f"ruta_larga (+{w.get('long_path', 2)})")
    if row.get("Profundidad", 0) > policies.get("deep_levels", 20):
        reasons.append(f"profundidad (+{w.get('deep_levels', 2)})")

    return ", ".join(reasons)


# ---------- HOTFIX pandas 2.x ----------
def size_buckets(df: pd.DataFrame, col: str = "TamanoBytes") -> pd.DataFrame:
    """
    Distribución por rangos de tamaño (orden lógico y compatible con pandas 1.x/2.x).
    Devuelve columnas: ['rango', 'conteo'].
    """
    if col not in df.columns:
        return pd.DataFrame(columns=["rango", "conteo"])

    s = pd.to_numeric(df[col], errors="coerce")

    bins = [0, 10 * 1024 ** 2, 100 * 1024 ** 2, 1024 ** 3, 10 * 1024 ** 3, np.inf]
    labels = ["0–10 MB", "10–100 MB", "100 MB–1 GB", "1–10 GB", "10+ GB"]

    cat = pd.cut(
        s,
        bins=bins,
        labels=labels,
        right=False,
        include_lowest=True,
        ordered=True
    )

    out = (
        cat.value_counts(sort=False)     # respeta el orden de labels
           .rename_axis("rango")
           .reset_index(name="conteo")
    )
    return out


def kpi_advanced(df: pd.DataFrame) -> pd.DataFrame:
    """
    KPIs de impacto para tablero ejecutivo.
    - Grupos duplicados y ahorro potencial (bytes)
    - Rutas largas (>255) / profundas (>20 niveles)
    - Antiguos (>365 días por la mejor fecha disponible: Acceso/Modificación/Creación)
    - Sin propietario / sin MIME
    """
    out = []
    n = len(df)

    def add(k, v):
        out.append({"KPI": k, "Valor": v})

    # Duplicados y ahorro potencial
    dup, esp = duplicates_by_hash(df)
    add("Grupos duplicados", int(len(dup)))
    add("Ahorro potencial (bytes)", f"{esp:,.0f}")

    # Rutas largas / profundas
    long_r = int((df.get("LongRuta", pd.Series([np.nan] * n)) > 255).sum()) if "LongRuta" in df.columns else 0
    deep = int((df.get("Profundidad", pd.Series([np.nan] * n)) > 20).sum()) if "Profundidad" in df.columns else 0
    add("Rutas largas (>255)", long_r)
    add("Rutas profundas (>20 niveles)", deep)

    # Antiguos (>365 días) según mejor fecha disponible
    base = None
    for col in ["FechaAcceso", "FechaModificacion", "FechaCreacion"]:
        if col in df.columns:
            base = pd.to_datetime(df[col], errors="coerce")
            try:
                base = base.tz_localize(None)
            except Exception:
                pass
            break
    if base is not None:
        stale = (pd.Timestamp.now().tz_localize(None) - base).dt.days > 365
        add("Antiguos (>365d)", int(stale.fillna(False).sum()))

    # Sin propietario / sin MIME
    if "Propietario" in df.columns:
        add("Sin propietario", int(df["Propietario"].isna().sum()))
    if "MimeType" in df.columns:
        add("Sin MIME", int(df["MimeType"].isna().sum()))

    return pd.DataFrame(out)


__all__ = [
    "overview_metrics",
    "top_n_by_size",
    "missingness",
    "freq_table",
    "duplicates_by_hash",
    "timeline_counts",
    "agg_by",
    "agg_by_folder",
    "explain_risk_row",
    "size_buckets",
    "kpi_advanced",
]
