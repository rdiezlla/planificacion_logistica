# README_ABC

## Objetivo del modulo

`abc_main.py` genera clasificacion ABC-XYZ de picking para priorizar SKU y apoyar decisiones de layout.

- `ABC`: importancia por `pick_lines` (no por unidades).
- `XYZ`: estabilidad/variabilidad semanal de `pick_lines`.

Este modulo forma parte del motor analitico y escribe resultados en `outputs_abc/`.

## Inputs

- `movimientos.xlsx` (raiz del repo)

## Ejecucion

### Mac/Linux

```bash
source .venv/bin/activate
python abc_main.py --input movimientos.xlsx --output_dir outputs_abc
```

### Windows PowerShell

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\activate
python abc_main.py --input movimientos.xlsx --output_dir outputs_abc
```

## Outputs principales

En `outputs_abc/`:

- `abc_picking_annual.csv`
- `abc_picking_quarterly.csv`
- `abc_picking_ytd.csv`
- `abc_summary_by_period.csv`
- `abc_xyz_summary_by_period.csv`
- `abc_owner_summary.csv`
- `abc_top_changes.csv`
- `abc_for_layout_candidates.csv`
- `plots/*.png`

## Conexion con el resto del proyecto

- No recalcula forecast de `main.py`; es un modulo complementario.
- Streamlit (pagina `ABC Picking`) consume estos CSV para visualizacion.
- La web React actual no consume `outputs_abc/`.

## Nota operativa

`owner_scope` permite analizar vision global (`GLOBAL`) y por propietario en los mismos outputs.
