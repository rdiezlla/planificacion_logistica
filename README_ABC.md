# README_ABC

## Objetivo del modulo

`abc_main.py` genera clasificacion ABC-XYZ de picking para priorizar SKU y apoyar decisiones de layout.

- `ABC`: importancia por `pick_lines` (no por unidades).
- `XYZ`: estabilidad/variabilidad semanal de `pick_lines`.
- `abc_pareto_class`: Pareto puro por acumulado.
- `abc_class`: clase operativa ajustada con suelo minimo de actividad para no inflar `A` con micro-rotacion.
- El modulo escribe resultados en `outputs_abc/`.

## Inputs

- Por defecto se usa `movimientos.xlsx` resuelto automaticamente desde OneDrive `Descargas BI`.
- El flujo crudo asociado vive en `Descargas BI/Movimientos/` y `Descargas BI/Movimientos pedidos/`.

## Ejecucion

### Mac/Linux

```bash
source .venv/bin/activate
python abc_main.py --output_dir outputs_abc
```

### Windows PowerShell

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\activate
python abc_main.py --output_dir outputs_abc
```

## Outputs principales

- `abc_picking_annual.csv`
- `abc_picking_quarterly.csv`
- `abc_picking_ytd.csv`
- `abc_summary_by_period.csv`
- `abc_xyz_summary_by_period.csv`
- `abc_owner_summary.csv`
- `abc_top_changes.csv`
- `abc_for_layout_candidates.csv`
- `plots/*.png`

## Cobertura del ultimo run

- Lineas PI validas con fecha: 65189
- SKUs analizados: 7795
- Owners analizados: 74
- Rango de fechas: 2022-01-03 00:00:00 -> 2026-03-05 00:00:00
- Registros descartados por fecha invalida: 0
- Ultimo periodo global disponible: 2026-YTD
- Concentracion de pick_lines en clase A (ultimo periodo): 68.4%

## Conexion con el resto del proyecto

- No recalcula el forecast principal de `main.py`.
- Streamlit (pagina `ABC Picking`) consume `outputs_abc/` para visualizacion.
- La web React actual no consume `outputs_abc/`.
