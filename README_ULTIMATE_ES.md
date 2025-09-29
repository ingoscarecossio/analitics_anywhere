# 🏆 Anywhere Analytics ULTIMATE — Inventarios de Archivos (ES)

**Nivel final (para este alcance)**: UI avanzada, **simulador de deduplicación**, explicabilidad del **RiskScore**,
**heatmaps**, **bookmarks de vistas** (guardar/cargar filtros), exportes **Excel + ZIP** con PNG/CSV y **Dockerfile**.

## Ejecutar
```bash
pip install -r requirements.txt
streamlit run app_ultimate.py
```
CLI:
```bash
python cli_ultimate.py report --input "inventario.xlsx" --output "./reportes"
python cli_ultimate.py delta  --input "hoy.xlsx" --baseline "ayer.xlsx" --output "./reportes"
python cli_ultimate.py simulate-dedupe --input "inventario.xlsx" --by CarpetaPadre --strategy keep-largest
```
Docker:
```bash
docker build -t anywhere-analytics-ultimate .
docker run -p 8501:8501 -v $PWD:/data anywhere-analytics-ultimate
```
