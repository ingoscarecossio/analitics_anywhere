# -*- coding: utf-8 -*-
import argparse, os, json
import pandas as pd
from ANALYTICS_ULT.io_utils import load_table, coerce_booleans, coerce_datetimes, coerce_numeric
from ANALYTICS_ULT.path_utils import split_path_to_levels
from ANALYTICS_ULT.analyzers import (top_n_by_size, missingness, freq_table, duplicates_by_hash, timeline_counts, agg_by_folder)
from ANALYTICS_ULT.mismatch import mime_ext_mismatch
from ANALYTICS_ULT.risk import risk_scoring, DEFAULT_POLICIES
from ANALYTICS_ULT.simulator import simulate_dedupe
from ANALYTICS_ULT.exporters import export_excel_with_figs

def _prep(path):
    df = load_table(path, sheet_name=None)
    df = coerce_datetimes(df, ["FechaCreacion","FechaModificacion","FechaAcceso"])
    df = coerce_numeric(df, ["TamanoBytes"])
    df = coerce_booleans(df, ["Oculto","SoloLectura"])
    if not any(c.startswith("Nivel_") for c in df.columns):
        if "RutaRelativa" in df.columns:
            df = pd.concat([df.reset_index(drop=True), split_path_to_levels(df["RutaRelativa"]).reset_index(drop=True)], axis=1)
    return df

def run_report(args):
    df = _prep(args.input)
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
        "RiskTop": risk_scoring(df, DEFAULT_POLICIES).head(1000),
    }
    out = export_excel_with_figs(tables, figures={}, out_dir=args.output, base_name="Reporte_Analitica_ULTIMATE")
    print("OK:", out)

def run_delta(args):
    df = _prep(args.input); base = _prep(args.baseline)
    key = "Hash" if "Hash" in df.columns else ("RutaCompleta" if "RutaCompleta" in df.columns else None)
    if key is None or key not in base.columns:
        print("No hay clave común (Hash o RutaCompleta)"); return
    cur = df.drop_duplicates(subset=[key]).set_index(key); bs = base.drop_duplicates(subset=[key]).set_index(key)
    add_keys = cur.index.difference(bs.index); rem_keys = bs.index.difference(cur.index); common = cur.index.intersection(bs.index)
    add = cur.loc[add_keys].reset_index(); rem = bs.loc[rem_keys].reset_index()
    chg = pd.DataFrame({"key": common, "TamanoBytes_cur": pd.to_numeric(cur.loc[common]["TamanoBytes"], errors="coerce"),
                        "TamanoBytes_base": pd.to_numeric(bs.loc[common]["TamanoBytes"], errors="coerce")})
    chg = chg[chg["TamanoBytes_cur"] != chg["TamanoBytes_base"]]
    with pd.ExcelWriter(os.path.join(args.output, "Delta_ULTIMATE.xlsx"), engine="xlsxwriter") as w:
        add.to_excel(w, sheet_name="Agregados", index=False)
        rem.to_excel(w, sheet_name="Removidos", index=False)
        chg.to_excel(w, sheet_name="Cambiados", index=False)
    print("OK: delta exportado")

def run_simulate_dedupe(args):
    df = _prep(args.input)
    plan, ahorro = simulate_dedupe(df, by=args.by, strategy=args.strategy)
    out = os.path.join(args.output, "plan_deduplicacion.csv")
    os.makedirs(args.output, exist_ok=True)
    plan.to_csv(out, index=False, encoding="utf-8")
    print(f"OK: ahorro={ahorro:.0f} bytes, plan={out}")

def main():
    ap = argparse.ArgumentParser(description="Anywhere Analytics ULTIMATE")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("report"); r.add_argument("--input", required=True); r.add_argument("--output", default="./reportes"); r.set_defaults(func=run_report)
    d = sub.add_parser("delta"); d.add_argument("--input", required=True); d.add_argument("--baseline", required=True); d.add_argument("--output", default="./reportes"); d.set_defaults(func=run_delta)
    s = sub.add_parser("simulate-dedupe"); s.add_argument("--input", required=True); s.add_argument("--by", default="CarpetaPadre"); s.add_argument("--strategy", default="keep-largest"); s.add_argument("--output", default="./reportes"); s.set_defaults(func=run_simulate_dedupe)
    args = ap.parse_args(); args.func(args)

if __name__ == "__main__":
    main()
