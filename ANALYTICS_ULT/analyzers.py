# -*- coding: utf-8 -*-
import pandas as pd, numpy as np
from .io_utils import human_bytes
from .path_utils import path_depth_from_levels
from .security import world_writable, world_readable

def overview_metrics(df):
    met = {"filas": len(df), "columnas": df.shape[1]}
    if "TamanoBytes" in df.columns:
        total = pd.to_numeric(df["TamanoBytes"], errors="coerce").sum()
        met.update({
            "tamano_total_bytes": float(total),
            "tamano_total_humano": human_bytes(total),
            "tamano_promedio": float(pd.to_numeric(df["TamanoBytes"], errors="coerce").mean()),
            "archivos_cero_bytes": int((pd.to_numeric(df["TamanoBytes"], errors="coerce")==0).sum())
        })
    if "Extension" in df.columns: met["extensiones_unicas"] = int(df["Extension"].astype(str).nunique(dropna=True))
    if "Hash" in df.columns: met["hash_nulos"] = int(df["Hash"].isna().sum())
    if "Oculto" in df.columns: met["porc_ocultos"] = float((df["Oculto"]==True).mean()*100)
    if "SoloLectura" in df.columns: met["porc_solo_lectura"] = float((df["SoloLectura"]==True).mean()*100)
    return met

def top_n_by_size(df, n=50):
    if "TamanoBytes" not in df.columns: return pd.DataFrame()
    t = df.copy()
    t["TamanoBytes"] = pd.to_numeric(t["TamanoBytes"], errors="coerce")
    return t.sort_values("TamanoBytes", ascending=False).head(n)

def missingness(df):
    m = df.isna().sum().to_frame("faltantes")
    m["porcentaje"] = (m["faltantes"]/len(df)*100).round(2)
    return m.sort_values("porcentaje", ascending=False)

def freq_table(df, col, n=30):
    if col not in df.columns: return pd.DataFrame()
    s = df[col].astype("string")
    tab = s.value_counts(dropna=False).to_frame("conteo")
    tab["porcentaje"] = (tab["conteo"]/len(s)*100).round(2)
    return tab.head(n).reset_index(names=col)

def duplicates_by_hash(df):
    use_col = "Hash" if "Hash" in df.columns else None
    if not use_col and set(["Nombre","TamanoBytes"]).issubset(df.columns):
        df = df.copy(); df["__PseudoHash__"] = df["Nombre"].astype(str)+"|"+df["TamanoBytes"].astype(str)
        use_col = "__PseudoHash__"
    if not use_col: return pd.DataFrame(), 0.0
    g = df.groupby(use_col, dropna=True, as_index=False).agg(conteo=("Nombre","size"),
                                                             tam_total=("TamanoBytes", lambda x: pd.to_numeric(x, errors="coerce").sum()))
    dup = g[g["conteo"]>1].sort_values(["conteo","tam_total"], ascending=[False, False])
    tam_uniq = df.dropna(subset=[use_col]).drop_duplicates(subset=[use_col])["TamanoBytes"]
    espacio_potencial = float(pd.to_numeric(df["TamanoBytes"], errors="coerce").sum() - pd.to_numeric(tam_uniq, errors="coerce").sum())
    return dup, max(0.0, espacio_potencial)

def timeline_counts(df, date_col, freq="M"):
    if date_col not in df.columns: return pd.DataFrame()
    s = pd.to_datetime(df[date_col], errors="coerce")
    try: s = s.dt.tz_localize(None)
    except Exception: pass
    gr = s.dt.to_period(freq).value_counts().sort_index()
    out = gr.rename_axis("periodo").reset_index(name="conteo"); out["periodo"]=out["periodo"].astype(str)
    return out

def agg_by(df, base_col, top=50):
    if base_col not in df.columns: return pd.DataFrame()
    t = df.copy()
    t["TamanoBytes"] = pd.to_numeric(t.get("TamanoBytes"), errors="coerce")
    g = t.groupby(base_col, dropna=False).agg(archivos=("Nombre","size"), tam_total=("TamanoBytes","sum")).reset_index().rename(columns={base_col:"Categoria"})
    g["tam_total_humano"] = g["tam_total"].map(human_bytes)
    return g.sort_values(["tam_total","archivos"], ascending=[False, False]).head(top)

def agg_by_folder(df, top=50):
    base_col = "CarpetaPadre" if "CarpetaPadre" in df.columns else None
    if not base_col:
        niveles = sorted([c for c in df.columns if c.startswith("Nivel_")], key=lambda x: int(x.split("_")[1]))
        base_col = niveles[-1] if niveles else None
    if not base_col: return pd.DataFrame()
    return agg_by(df, base_col, top=top)

def explain_risk_row(row, policies):
    reasons = []
    w = policies.get("weights", {})
    if not pd.isna(row.get("TamanoBytes")) and row["TamanoBytes"] >= policies.get("big_bytes", 2*1024**3):
        reasons.append(f"tamaño≥{policies.get('big_bytes')} (+{w.get('big_file',3)})")
    if str(row.get("Perm_RWX") or "").endswith("w"):
        reasons.append(f"world-writable (+{w.get('world_writable',4)})")
    elif str(row.get("Perm_RWX") or "").endswith("r"):
        reasons.append(f"world-readable (+{w.get('world_readable',1)})")
    if row.get("Oculto") is True:
        reasons.append(f"oculto (+{w.get('hidden',1)})")
    if row.get("LongRuta",0) > policies.get("long_path",255):
        reasons.append(f"ruta_larga (+{w.get('long_path',2)})")
    if row.get("Profundidad",0) > policies.get("deep_levels",20):
        reasons.append(f"profundidad (+{w.get('deep_levels',2)})")
    # stale lo decide risk module; aquí solo texto si fecha vieja
    return ", ".join(reasons)


def size_buckets(df, col="TamanoBytes"):
    import pandas as pd, numpy as np
    if col not in df.columns: 
        return pd.DataFrame()
    s = pd.to_numeric(df[col], errors="coerce")
    bins = [0, 10*1024**2, 100*1024**2, 1024**3, 10*1024**3, np.inf]
    labels = ["0–10 MB","10–100 MB","100 MB–1 GB","1–10 GB","10+ GB"]
    cat = pd.cut(s, bins=bins, labels=labels, right=False, include_lowest=True)
    out = cat.value_counts().sort_index().reset_index().rename(columns={"index":"rango","count":"conteo"})
    out["conteo"] = out[0] if 0 in out.columns else out.get("count", out.iloc[:,1])
    out = out[["rango","conteo"]]
    return out

def kpi_advanced(df):
    import pandas as pd, numpy as np
    out = []
    n = len(df)
    def add(k, v): out.append({"KPI":k, "Valor":v})
    # duplicados y ahorro
    dup, esp = duplicates_by_hash(df)
    add("Grupos duplicados", len(dup))
    add("Ahorro potencial (bytes)", f"{esp:,.0f}")
    # rutas largas / profundas
    long_r = (df.get("LongRuta", pd.Series([np.nan]*n)) > 255).sum() if "LongRuta" in df.columns else 0
    deep = (df.get("Profundidad", pd.Series([np.nan]*n)) > 20).sum() if "Profundidad" in df.columns else 0
    add("Rutas largas (>255)", int(long_r))
    add("Rutas profundas (>20 niveles)", int(deep))
    # archivos antiguos (sin acceso reciente 365d aprox)
    for col in ["FechaAcceso","FechaModificacion","FechaCreacion"]:
        if col in df.columns:
            s = pd.to_datetime(df[col], errors="coerce")
            try: s = s.dt.tz_localize(None)
            except Exception: pass
            stale = (pd.Timestamp.now().tz_localize(None) - s).dt.days > 365
            add(f"Antiguos por {col} (>365d)", int(stale.sum()))
            break
    # sin propietario / sin mime
    if "Propietario" in df.columns: add("Sin propietario", int(df["Propietario"].isna().sum()))
    if "MimeType" in df.columns: add("Sin MIME", int(df["MimeType"].isna().sum()))
    return pd.DataFrame(out)
