# -*- coding: utf-8 -*-
"""
Anywhere Analytics ULTIMATE — App Streamlit (versión corregida)

- KPIs+ con categorías, buckets de tamaño y KPIs avanzados
- Gráficos legibles (smart_time_series, bar_top)
- Sin re-selecciones peligrosas de columnas (evita KeyError)
"""

import os
import json
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime

# --- Módulos del paquete ULTIMATE ---
from ANALYTICS_ULT.io_utils import (
    load_table, coerce_booleans, coerce_datetimes, coerce_numeric
)
from ANALYTICS_ULT.path_utils import split_path_to_levels, path_depth_from_levels
from ANALYTICS_ULT.analyzers import (
    overview_metrics, top_n_by_size, missingness, freq_table, duplicates_by_hash,
    timeline_counts, agg_by_folder, agg_by, size_buckets, kpi_advanced
)
from ANALYTICS_ULT.mismatch import mime_ext_mismatch
from ANALYTICS_ULT.validators import validate_sizes, validate_dates, anomalies_size_iqr
from ANALYTICS_ULT.security import octal_to_rwx
from ANALYTICS_ULT.risk import risk_scoring, DEFAULT_POLICIES
from ANALYTICS_ULT.simulator import simulate_dedupe
from ANALYTICS_ULT.viz import (
    bar_chart, line_chart, hist_log_sizes, treemap_sliced, heatmap_pivot,
    smart_time_series, bar_top
)
from ANALYTICS_ULT.categorize import add_category_column
from ANALYTICS_ULT.exporters import export_excel_with_figs

# ------------------------ Configuración UI ------------------------
st.set_page_config(page_title="Anywhere Analytics ULTIMATE", layout="wide")
st.title("🏆 Anywhere Analytics ULTIMATE")
st.caption("UI avanzada + simulador de deduplicación + explicabilidad de riesgos + exportes ejecutivos.")

# ------------------------ Sidebar ------------------------
with st.sidebar:
    st.header("📁 Datos de entrada")
    default_path = st.text_input("Ruta por defecto (opcional):", value="")
    uploaded = st.file_uploader("Corte actual (Excel/CSV)", type=["xlsx", "xls", "xlsm", "xlsb", "csv", "txt"])
    baseline = st.file_uploader("Corte base (opcional)", type=["xlsx", "xls", "xlsm", "xlsb", "csv", "txt"])
    sheet_name = st.text_input("Hoja (si Excel):", value="")
    sep = st.text_input("Separador (si CSV):", value=",")
    encoding = st.text_input("Encoding (si CSV):", value="utf-8")

    st.markdown("---")
    st.header("🧠 Policies (Risk)")
    policies_json = st.text_area("JSON de policies", value=json.dumps(DEFAULT_POLICIES, indent=2), height=280)

    st.markdown("---")
    st.header("🔖 Bookmarks")
    if "bookmark" not in st.session_state:
        st.session_state["bookmark"] = {}
    bookmark_name = st.text_input("Nombre de bookmark", value="mi_vista")
    if st.button("Guardar bookmark"):
        st.session_state["bookmark"][bookmark_name] = {
            "columns": [],
            "meta": {"saved_at": datetime.now().isoformat()}
        }
    if st.button("Exportar bookmarks JSON"):
        st.download_button(
            "Descargar JSON",
            data=json.dumps(st.session_state["bookmark"], indent=2).encode("utf-8"),
            file_name="bookmarks.json"
        )

# ------------------------ Carga y normalización ------------------------
def _load_dataframe(file, default_path, sheet_name, sep, encoding):
    if file is not None:
        tmp_dir = os.path.join(".", "__tmp__")
        os.makedirs(tmp_dir, exist_ok=True)
        in_path = os.path.join(tmp_dir, file.name)
        with open(in_path, "wb") as f:
            f.write(file.getbuffer())
        df = load_table(in_path, sheet_name=sheet_name or None, sep=sep or ",", encoding=encoding or "utf-8")
    elif default_path and os.path.exists(default_path):
        df = load_table(default_path, sheet_name=sheet_name or None, sep=sep or ",", encoding=encoding or "utf-8")
    else:
        st.stop()

    df = coerce_datetimes(df, ["FechaCreacion", "FechaModificacion", "FechaAcceso"])
    df = coerce_numeric(df, ["TamanoBytes"])
    df = coerce_booleans(df, ["Oculto", "SoloLectura"])

    # Niveles de ruta y métricas
    if not any(c.startswith("Nivel_") for c in df.columns):
        if "RutaRelativa" in df.columns:
            levels = split_path_to_levels(df["RutaRelativa"])
            df = pd.concat([df.reset_index(drop=True), levels.reset_index(drop=True)], axis=1)

    df["Profundidad"] = path_depth_from_levels(df)

    base_ruta = "RutaCompleta" if "RutaCompleta" in df.columns else ("RutaRelativa" if "RutaRelativa" in df.columns else None)
    df["LongRuta"] = df[base_ruta].astype(str).str.len() if base_ruta else np.nan

    if "PermOctal" in df.columns:
        df["Perm_RWX"] = df["PermOctal"].apply(octal_to_rwx)

    # Categoría (Imagen, Video, Documento, etc.)
    df = add_category_column(df)
    return df

df = _load_dataframe(uploaded, default_path, sheet_name, sep, encoding)
df_base = _load_dataframe(baseline, default_path, sheet_name, sep, encoding) if baseline is not None else None

# ------------------------ Tabs ------------------------
tab_dash, tab_kpis, tab_risk, tab_dup, tab_folders, tab_heatmap, tab_time, tab_quality, tab_mismatch, tab_delta, tab_validate, tab_export = st.tabs([
    "Dashboard", "KPIs+", "Riesgos", "Duplicados/Simulador", "Carpetas", "Heatmap", "Temporal",
    "Calidad", "MIME vs Ext", "Delta", "Validaciones", "Exportar"
])

# ------------------------ Dashboard ------------------------
with tab_dash:
    st.subheader("KPIs")
    met = overview_metrics(df)
    c = st.columns(4)
    c[0].metric("Archivos", f"{met.get('filas', len(df)):,}")
    c[1].metric("Tamaño total", met.get("tamano_total_humano", "—"))
    c[2].metric("0 bytes", f"{met.get('archivos_cero_bytes', 0):,}")
    c[3].metric("Extensiones únicas", f"{met.get('extensiones_unicas', '—')}")
    st.markdown("**Top por tamaño**")
    st.dataframe(top_n_by_size(df, n=50), use_container_width=True, height=360)
    if "TamanoBytes" in df.columns:
        fig = hist_log_sizes(df["TamanoBytes"])
        if fig is not None:
            st.pyplot(fig, use_container_width=True)

# ------------------------ KPIs+ ------------------------
with tab_kpis:
    st.subheader("KPIs de impacto y categorías")

    # Conteo por Categoría
    if "Categoria" in df.columns:
        cat_counts = df["Categoria"].value_counts(dropna=False).reset_index().rename(columns={"index": "Categoria", "Categoria": "conteo"})
        st.markdown("**Archivos por categoría**")
        st.dataframe(cat_counts, use_container_width=True, height=260)
        try:
            st.pyplot(bar_top(cat_counts, "Categoria", "conteo", "Top categorías por número de archivos", horizontal=True), use_container_width=True)
        except Exception:
            pass

        # Tamaño por categoría
        if "TamanoBytes" in df.columns:
            tmp = df.copy()
            tmp["TamanoBytes"] = pd.to_numeric(tmp["TamanoBytes"], errors="coerce")
            cat_size = tmp.groupby("Categoria", dropna=False)["TamanoBytes"].sum().sort_values(ascending=False).reset_index()
            st.markdown("**Tamaño total (bytes) por categoría**")
            st.dataframe(cat_size, use_container_width=True, height=260)

    # Buckets de tamaño (SIEMPRE columnas ['rango','conteo'])
    sb = size_buckets(df)
    st.markdown("**Distribución por rangos de tamaño**")
    st.dataframe(sb, use_container_width=True, height=200)
    try:
        st.pyplot(bar_top(sb, "rango", "conteo", "Archivos por rango de tamaño"), use_container_width=True)
    except Exception:
        pass

    # KPIs avanzados
    st.markdown("**KPIs Avanzados**")
    st.dataframe(kpi_advanced(df), use_container_width=True, height=240)

# ------------------------ Riesgos ------------------------
with tab_risk:
    st.subheader("Priorización + explicación")
    try:
        policies = json.loads(policies_json)
    except Exception as e:
        st.error(f"Policies JSON inválido: {e}")
        policies = DEFAULT_POLICIES
    scored = risk_scoring(df, policies)
    cols_show = [c for c in ["Nombre", "TamanoBytes", "Perm_RWX", "LongRuta", "Profundidad", "RiskScore", "RiskBand", "RiskWhy"] if c in scored.columns]
    st.dataframe(scored[cols_show].head(1000), use_container_width=True, height=420)

# ------------------------ Duplicados / Simulador ------------------------
with tab_dup:
    st.subheader("Duplicados por Hash/PseudoHash + Simulador")
    dup, espacio = duplicates_by_hash(df)
    st.write(f"**Espacio potencial recuperable (estimado):** {espacio:,.0f} bytes")
    st.dataframe(dup, use_container_width=True, height=300)

    st.markdown("---")
    st.markdown("### Simulador de deduplicación")
    by_opts = [c for c in ["CarpetaPadre", "Propietario", "Extension", "Raiz"] if c in df.columns]
    by = st.selectbox("Agrupar por", options=by_opts or ["CarpetaPadre"])
    strat = st.selectbox("Estrategia", options=["keep-largest", "keep-earliest", "keep-latest"])
    if st.button("Simular"):
        plan, ahorro = simulate_dedupe(df, by=by, strategy=strat)
        st.metric("Ahorro estimado", f"{ahorro:,.0f} bytes")
        st.dataframe(plan.head(1000), use_container_width=True, height=360)
        if not plan.empty:
            st.download_button("Descargar plan CSV", data=plan.to_csv(index=False).encode("utf-8"), file_name="plan_deduplicacion.csv")

# ------------------------ Carpetas / Raíz / Extensión ------------------------
with tab_folders:
    st.subheader("Agregación por carpeta / raíz / extensión")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Por carpeta (Top 50)**")
        st.dataframe(agg_by_folder(df, top=50), use_container_width=True, height=360)
    with c2:
        if "Raiz" in df.columns:
            st.markdown("**Por raíz**")
            st.dataframe(agg_by(df, "Raiz", top=50), use_container_width=True, height=360)
        if "Extension" in df.columns:
            st.markdown("**Por extensión (Top 30)**")
            st.dataframe(agg_by(df, "Extension", top=30), use_container_width=True, height=360)

    if "CarpetaPadre" in df.columns:
        tt = df.groupby("CarpetaPadre")["TamanoBytes"].sum().sort_values(ascending=False).head(25)
        fig = treemap_sliced(tt.values, tt.index, title="Treemap - Top carpetas por tamaño")
        if fig is not None:
            st.pyplot(fig, use_container_width=True)

# ------------------------ Heatmap ------------------------
with tab_heatmap:
    st.subheader("Heatmap de tamaños por Propietario vs Extensión")
    if "Propietario" in df.columns and "Extension" in df.columns and "TamanoBytes" in df.columns:
        pv = pd.pivot_table(
            df,
            values="TamanoBytes",
            index="Propietario",
            columns="Extension",
            aggfunc=lambda x: pd.to_numeric(x, errors="coerce").sum()
        ).fillna(0)
        # Reducir dimensiones para legibilidad
        pv = pv.sort_values(by=list(pv.columns), ascending=False).head(30)
        if pv.shape[0] > 0 and pv.shape[1] > 0:
            fig = heatmap_pivot(pv, title="Tamaño total (bytes)")
            st.pyplot(fig, use_container_width=True)
        st.dataframe(pv, use_container_width=True, height=360)
    else:
        st.info("Se requieren columnas Propietario, Extension y TamanoBytes.")

# ------------------------ Temporal ------------------------
with tab_time:
    st.subheader("Series temporales")
    for label in ["FechaCreacion", "FechaModificacion", "FechaAcceso"]:
        if label in df.columns:
            t = timeline_counts(df, label, "M")
            if not t.empty:
                st.markdown(f"**{label} (mensual)**")
                st.dataframe(t, use_container_width=True, height=240)
                st.pyplot(smart_time_series(t, "periodo", "conteo", f"Conteo mensual — {label}"), use_container_width=True)

# ------------------------ Calidad ------------------------
with tab_quality:
    st.subheader("Calidad de datos")
    st.dataframe(missingness(df), use_container_width=True, height=360)

# ------------------------ MIME vs Ext ------------------------
with tab_mismatch:
    st.subheader("MIME vs Extensión")
    st.dataframe(mime_ext_mismatch(df), use_container_width=True, height=420)

# ------------------------ Delta ------------------------
with tab_delta:
    st.subheader("Delta (corte vs base)")
    if df_base is None:
        st.info("Cargue un corte base en la barra lateral para activar el delta.")
    else:
        key = "Hash" if "Hash" in df.columns else ("RutaCompleta" if "RutaCompleta" in df.columns else None)
        if key is None or key not in df_base.columns:
            st.warning("No hay columna clave común (Hash o RutaCompleta) para delta.")
        else:
            cur = df.drop_duplicates(subset=[key]).set_index(key)
            base = df_base.drop_duplicates(subset=[key]).set_index(key)
            add_keys = cur.index.difference(base.index)
            rem_keys = base.index.difference(cur.index)
            common = cur.index.intersection(base.index)

            add = cur.loc[add_keys].reset_index()
            rem = base.loc[rem_keys].reset_index()
            chg = pd.DataFrame({
                "key": common,
                "TamanoBytes_cur": pd.to_numeric(cur.loc[common]["TamanoBytes"], errors="coerce"),
                "TamanoBytes_base": pd.to_numeric(base.loc[common]["TamanoBytes"], errors="coerce")
            })
            chg = chg[chg["TamanoBytes_cur"] != chg["TamanoBytes_base"]]

            c1, c2, c3 = st.columns(3)
            c1.metric("Agregados", len(add))
            c2.metric("Removidos", len(rem))
            c3.metric("Cambiados", len(chg))

            st.markdown("**Agregados**")
            st.dataframe(add, use_container_width=True, height=240)
            st.markdown("**Removidos**")
            st.dataframe(rem, use_container_width=True, height=240)
            st.markdown("**Cambiados**")
            st.dataframe(chg, use_container_width=True, height=240)

# ------------------------ Validaciones ------------------------
with tab_validate:
    st.subheader("Validaciones")
    v1 = validate_sizes(df)
    v2 = validate_dates(df)
    v3 = anomalies_size_iqr(df)
    c1, c2, c3 = st.columns(3)
    c1.metric("Tamaños negativos", len(v1))
    c2.metric("Fechas inválidas", len(v2))
    c3.metric("Anomalías IQR", len(v3))
    st.markdown("**Fechas inválidas**")
    st.dataframe(v2, use_container_width=True, height=240)
    st.markdown("**Tamaños negativos**")
    st.dataframe(v1, use_container_width=True, height=200)
    st.markdown("**Anomalías IQR**")
    st.dataframe(v3, use_container_width=True, height=240)

# ------------------------ Exportar ------------------------
with tab_export:
    st.subheader("Exportes (Excel + PNG)")
    out_dir = st.text_input("Directorio de salida", value=os.getcwd())
    base_name = st.text_input("Nombre base", value=f"Reporte_Analitica_ULTIMATE_{datetime.now().strftime('%Y%m%d_%H%M%S')}")

    if st.button("Generar Excel + Visuales"):
        figures = {}

        if "TamanoBytes" in df.columns:
            f = hist_log_sizes(df["TamanoBytes"])
            if f is not None:
                figures["hist_tamano"] = f

        if "CarpetaPadre" in df.columns:
            tt = df.groupby("CarpetaPadre")["TamanoBytes"].sum().sort_values(ascending=False).head(25)
            tf = treemap_sliced(tt.values, tt.index, title="Treemap - Top carpetas por tamaño")
            if tf is not None:
                figures["treemap_carpetas"] = tf

        # Series temporales con helper legible
        for col in ["FechaCreacion", "FechaModificacion", "FechaAcceso"]:
            if col in df.columns:
                t = timeline_counts(df, col, "M")
                if not t.empty:
                    figures[f"ts_{col}"] = smart_time_series(t, "periodo", "conteo", f"Conteo mensual — {col}")

        # Gráfico categorías (conteo)
        if "Categoria" in df.columns:
            cat_counts = df["Categoria"].value_counts(dropna=False).reset_index().rename(columns={"index": "Categoria", "Categoria": "conteo"})
            try:
                figures["cat_counts"] = bar_top(cat_counts, "Categoria", "conteo", "Top categorías por número de archivos", horizontal=True)
            except Exception:
                pass

        tables = {
            "ResumenTop": top_n_by_size(df, n=50),
            "CalidadDatos": missingness(df),
            "TopExtensiones": freq_table(df, "Extension", n=50),
            "TopMIME": freq_table(df, "MimeType", n=50),
            "TopPropietario": freq_table(df, "Propietario", n=50),
            "Duplicados": duplicates_by_hash(df)[0],
            "Carpetas": agg_by_folder(df, top=50),
            "TimelineCreacion": timeline_counts(df, "FechaCreacion", "M"),
            "TimelineModificacion": timeline_counts(df, "FechaModificacion", "M"),
            "TimelineAcceso": timeline_counts(df, "FechaAcceso", "M"),
            "MIME_Ext_Mismatch": mime_ext_mismatch(df),
            "RiskTop": risk_scoring(df, json.loads(policies_json)).head(1000) if policies_json else risk_scoring(df, DEFAULT_POLICIES).head(1000),
            "Categorias_Conteo": df["Categoria"].value_counts(dropna=False).reset_index().rename(columns={"index": "Categoria", "Categoria": "conteo"}) if "Categoria" in df.columns else pd.DataFrame(),
            "Categorias_Tamano": (pd.DataFrame({"Categoria": df.get("Categoria", pd.Series(index=df.index)),
                                                "TamanoBytes": pd.to_numeric(df.get("TamanoBytes", pd.Series(index=df.index)), errors="coerce")})
                                  .groupby("Categoria").sum().reset_index() if "Categoria" in df.columns else pd.DataFrame()),
            "Size_Buckets": size_buckets(df),
            "KPIs_Avanzados": kpi_advanced(df),
        }

        try:
            out_path = export_excel_with_figs(tables, figures, out_dir, base_name)
            st.success(f"Excel generado: {out_path}")
            with open(out_path, "rb") as f:
                st.download_button("Descargar Excel", data=f.read(), file_name=os.path.basename(out_path))
        except Exception as e:
            st.error(f"No se pudo exportar: {e}")
