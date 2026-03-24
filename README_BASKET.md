# README_BASKET

## Objetivo del modulo

`basket_main.py` genera analitica de co-ocurrencia para optimizacion de picking y layout.

Regla de negocio central:

- Para layout, priorizar nivel operativo `pedido x propietario` (`oper`).
- El nivel `order` sirve como contraste.
- El modulo escribe resultados en `outputs_basket/`.

## Inputs

- Por defecto se usa `movimientos.xlsx` resuelto automaticamente desde OneDrive `Descargas BI`.
- El flujo crudo asociado vive en `Descargas BI/Movimientos/` y `Descargas BI/Movimientos pedidos/`.

## Ejecucion

### Mac/Linux

```bash
source .venv/bin/activate
python basket_main.py --output_dir outputs_basket
```

### Windows PowerShell

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\activate
python basket_main.py --output_dir outputs_basket
```

## Outputs principales

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

## Cobertura del ultimo run

- Filas raw movimientos: 278445
- Filas PI: 65204
- Filas PI validas: 65189
- % propietario desconocido: 0.00%
- % missing ubicacion: 0.42%
- Filas en `top_pairs_oper.csv`: 113622
- Filas en `top_triples_oper.csv`: 576784
- Transacciones oper: 21201
- Transacciones order: 17638

## Conexion con el resto del proyecto

- No recalcula el forecast principal de `main.py`.
- Streamlit (pagina `Optimizacion picking`) consume `outputs_basket/`.
- La web React actual no consume `outputs_basket/`.
