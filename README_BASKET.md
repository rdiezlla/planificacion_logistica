# README_BASKET

## Objetivo del modulo

`basket_main.py` genera analitica de co-ocurrencia para optimizacion de picking y layout.

Regla de negocio central:

- Para layout, priorizar nivel operativo `pedido x propietario` (`oper`).
- El nivel `order` sirve como contraste.

Este modulo forma parte del motor analitico y escribe resultados en `outputs_basket/`.

## Inputs

- `movimientos.xlsx` (raiz del repo)

## Ejecucion

### Mac/Linux

```bash
source .venv/bin/activate
python basket_main.py --input movimientos.xlsx --output_dir outputs_basket
```

### Windows PowerShell

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\activate
python basket_main.py --input movimientos.xlsx --output_dir outputs_basket
```

## Outputs principales

En `outputs_basket/`:

- `transactions_summary_oper.csv`
- `transactions_summary_order.csv`
- `sku_frequency_oper.csv`
- `sku_frequency_order.csv`
- `top_pairs_oper.csv`
- `top_pairs_order.csv`
- `rules_oper.csv`
- `rules_order.csv`
- `top_triples_oper.csv`
- `top_triples_order.csv`
- `sku_clusters_oper.csv`
- `sku_clusters_order.csv`
- `order_owner_penalty.csv`
- `owner_penalty_kpis.csv`
- `location_savings_oper.csv` (si hay ubicaciones validas)
- `plots/*.png`

## Conexion con el resto del proyecto

- No recalcula forecast de `main.py`; es un modulo complementario.
- Streamlit (pagina `Optimizacion picking`) consume estos CSV.
- La web React actual no consume `outputs_basket/`.

## Nota operativa

Si faltan algunos CSV opcionales (por ejemplo `sku_neighbors.csv`), Streamlit muestra aviso y sigue funcionando.
