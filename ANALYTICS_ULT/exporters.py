# -*- coding: utf-8 -*-
import os, zipfile
import pandas as pd
import matplotlib.pyplot as plt

def _sanitize_tz(df: pd.DataFrame):
    for col in df.columns:
        if str(df[col].dtype).startswith("datetime64[ns,") or getattr(getattr(df[col], 'dt', None), 'tz', None) is not None:
            try: df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(None)
            except Exception: pass
    return df

def export_excel_with_figs(tables: dict, figures: dict, out_dir: str, base_name="Reporte_Analitica_ULTIMATE"):
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{base_name}.xlsx")
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        wb = writer.book
        for sheet, df in tables.items():
            if df is None or (hasattr(df, "empty") and df.empty): continue
            name = (sheet[:28]+"...") if len(sheet)>31 else sheet
            df = _sanitize_tz(df.copy())
            df.to_excel(writer, sheet_name=name, index=False)
            ws = writer.sheets[name]; ws.set_zoom(110); ws.set_column(0,0,26); ws.set_column(1,50,18)
            if "RiskScore" in df.columns:
                ws.conditional_format(1, df.columns.get_loc("RiskScore"), min(1000,len(df)+1), df.columns.get_loc("RiskScore"), {"type":"3_color_scale"})
            if "conteo" in df.columns:
                ws.conditional_format(1, df.columns.get_loc("conteo"), min(1000,len(df)+1), df.columns.get_loc("conteo"), {"type":"3_color_scale"})
        if figures:
            ws = wb.add_worksheet("ResumenVisual")
            r=1; c=1
            for key, fig in figures.items():
                img_path = os.path.join(out_dir, f"{base_name}_{key}.png")
                try:
                    fig.savefig(img_path, dpi=150, bbox_inches="tight")
                    plt.close(fig)
                    ws.insert_image(r, c, img_path, {"x_scale":1.0, "y_scale":1.0})
                    r += 22
                except Exception:
                    pass
    return out_path

def export_zip_bundle(excel_path: str, csv_dict: dict, png_paths: list, out_zip_path: str):
    with zipfile.ZipFile(out_zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        if excel_path and os.path.exists(excel_path):
            z.write(excel_path, arcname=os.path.basename(excel_path))
        for name, path in csv_dict.items():
            if path and os.path.exists(path):
                z.write(path, arcname=os.path.join("csv", os.path.basename(path)))
        for p in png_paths:
            if p and os.path.exists(p):
                z.write(p, arcname=os.path.join("figs", os.path.basename(p)))
    return out_zip_path
