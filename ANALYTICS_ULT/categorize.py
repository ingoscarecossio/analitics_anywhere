# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

EXT_CATS = {
    # imágenes
    "jpg":"Imagen","jpeg":"Imagen","png":"Imagen","gif":"Imagen","bmp":"Imagen","tif":"Imagen","tiff":"Imagen","svg":"Imagen","webp":"Imagen","heic":"Imagen",
    # video
    "mp4":"Video","mov":"Video","avi":"Video","mkv":"Video","webm":"Video","wmv":"Video","flv":"Video","m4v":"Video",
    # audio
    "mp3":"Audio","wav":"Audio","flac":"Audio","aac":"Audio","ogg":"Audio","m4a":"Audio",
    # documentos
    "pdf":"Documento","doc":"Documento","docx":"Documento","xls":"Documento","xlsx":"Documento","ppt":"Documento","pptx":"Documento","odt":"Documento","ods":"Documento","odp":"Documento","rtf":"Documento","txt":"Documento","csv":"Documento",
    # código
    "py":"Código","js":"Código","ts":"Código","java":"Código","c":"Código","cpp":"Código","cs":"Código","go":"Código","rb":"Código","php":"Código","html":"Código","css":"Código","json":"Código","xml":"Código","yml":"Código","yaml":"Código","sql":"Código","sh":"Código","bat":"Código","ps1":"Código",
    # comprimidos
    "zip":"Comprimido","rar":"Comprimido","7z":"Comprimido","tar":"Comprimido","gz":"Comprimido","bz2":"Comprimido","xz":"Comprimido","iso":"Comprimido",
    # ejecutables/bibliotecas
    "exe":"Ejecutable","dll":"Ejecutable","so":"Ejecutable","dmg":"Ejecutable","app":"Ejecutable","msi":"Ejecutable",
    # bases de datos
    "db":"BaseDatos","sqlite":"BaseDatos","mdb":"BaseDatos","accdb":"BaseDatos","parquet":"BaseDatos","feather":"BaseDatos","orc":"BaseDatos",
    # CAD/GIS
    "dwg":"CAD/GIS","dxf":"CAD/GIS","shp":"CAD/GIS","kml":"CAD/GIS","kmz":"CAD/GIS","geojson":"CAD/GIS",
    # diseño
    "psd":"Diseño","ai":"Diseño","indd":"Diseño","sketch":"Diseño","fig":"Diseño"
}

def detect_category_row(mime, ext):
    m = str(mime or "").lower()
    e = str(ext or "").strip().lstrip(".").lower()
    if "/" in m:
        major = m.split("/",1)[0]
        if   major=="image": return "Imagen"
        elif major=="video": return "Video"
        elif major=="audio": return "Audio"
        elif major=="text": return "Texto"
        elif major=="application": 
            # si es 'application' inferimos por extensión para algo más útil
            pass
        elif major=="font": return "Fuente"
        elif major=="model": return "3D/Model"
        elif major=="message": return "Mensaje"
        elif major=="multipart": return "Multipart"
    if e in EXT_CATS: 
        return EXT_CATS[e]
    if m.startswith("application/"):
        # heurística por subtipos comunes
        if any(k in m for k in ["pdf","msword","officedocument","vnd.ms","csv","rtf"]): return "Documento"
        if any(k in m for k in ["zip","7z","tar","gzip","rar"]): return "Comprimido"
        if any(k in m for k in ["json","xml"]): return "Código"
    return "Otros"

def add_category_column(df: pd.DataFrame):
    mime_col = "MimeType" if "MimeType" in df.columns else None
    ext_col = "Extension" if "Extension" in df.columns else None
    if not mime_col and not ext_col:
        df["Categoria"] = "Otros"
        return df
    df = df.copy()
    df["Categoria"] = [detect_category_row(df.get(mime_col, pd.Series([None]*len(df))).iloc[i] if mime_col else None,
                                           df.get(ext_col, pd.Series([None]*len(df))).iloc[i] if ext_col else None)
                       for i in range(len(df))]
    return df
