# ABC Picking + XYZ

Analisis Pareto / clasificacion ABC-XYZ de picking basado en `pick_lines` (numero de movimientos PI por SKU).

## Base metodologica

- `ABC` se calcula por `pick_lines`, no por unidades. Esto refleja mejor la carga operativa real del almacen.
- `XYZ` se calcula con la variabilidad semanal de `pick_lines` por SKU dentro de cada periodo.
- `X`: estable (`cv_weekly <= 0.50`), `Y`: variabilidad media, `Z`: alta volatilidad.
- Regla robusta: si un SKU tiene menos de 3 semanas activas se marca `LOW_HISTORY`; si el promedio semanal es 0 se marca `UNKNOWN`.

## Filtro por propietario

- El pipeline genera vision `GLOBAL` y vision por propietario en los mismos CSV, usando `owner_scope`.
- El filtro por propietario afecta al calculo, no solo a la visualizacion.

## Cobertura del analisis

- Lineas PI validas con fecha: 65189
- SKUs analizados: 7795
- Owners analizados: 74
- Rango de fechas: 2022-01-03 00:00:00 -> 2026-03-05 00:00:00
- Registros descartados por fecha invalida: 0

## Interpretacion operativa de ABC-XYZ

- `AX`: alta rotacion y estable. Primeras posiciones / zonas calientes muy claras.
- `AY`: alta rotacion con variabilidad media. Mantener muy accesible y monitorizar cambios.
- `AZ`: alta rotacion pero volatil. Mantener premium, con flexibilidad ante picos.
- `BX`: accesible y estable, sin consumir tanto espacio premium.
- `BZ`: rotacion media y volatil. Monitorizar antes de fijar layout definitivo.
- `CZ`: baja prioridad y alta volatilidad. Revisar espacio y evitar sobreasignacion.

Ultimo periodo global disponible: `2026-YTD`
Concentracion de pick_lines en clase A: `80.0%`

## Propietarios con mayor peso

| owner_scope | n_skus | total_pick_lines | pct_total_pick_lines | top_sku | top_sku_pick_lines |
| --- | --- | --- | --- | --- | --- |
| GLOBAL | 7795 | 65189 | 1.0 | 011014 | 2775 |
| 23 | 1324 | 10709 | 0.16427618156437435 | 011014 | 1286 |
| 24 | 673 | 9726 | 0.1491969504057433 | 031045 | 739 |
| 3 | 883 | 7840 | 0.12026568899660986 | 012012 | 599 |
| 4 | 566 | 6285 | 0.0964119713448588 | 018000 | 631 |
| 29 | 378 | 3808 | 0.058414763226924786 | 011014 | 637 |
| 82 | 91 | 3666 | 0.05623648161499639 | 065072 | 663 |
| 5 | 428 | 2878 | 0.044148552669928974 | 012025 | 190 |
| 14 | 600 | 2137 | 0.03278160425838715 | 102183 | 25 |
| 30 | 216 | 1852 | 0.028409701023178757 | 016010 | 118 |
| 33 | 144 | 1833 | 0.028118240807498196 | 124139 | 224 |

## Candidatos de layout (GLOBAL, ultimo periodo)

| owner_scope | sku | denominacion | latest_period_type | latest_period | latest_abc_class | xyz_class | abc_xyz_class | latest_pick_lines | latest_pick_qty | latest_n_orders | latest_rank | mean_weekly_pick_lines | std_weekly_pick_lines | cv_weekly | n_weeks_observed | change_vs_prev_period | recommendation_tag |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| GLOBAL | 019080 | TINTO DE VERANO CON LIMON CACHIS MARIANO | ytd | 2026-YTD | A | Z | AZ | 223 | 8052.0 | 219 | 1 | 22.3 | 26.769572278988694 | 1.2004292501788651 | 5 | SIN_REFERENCIA | KEEP_FRONT_FLEX |
| GLOBAL | 018428 | AGUA 50CL PLÁSTICO SOLAN 2025 CALCIO, MA | ytd | 2026-YTD | A | Y | AY | 124 | 27531.7 | 83 | 2 | 12.4 | 7.964923100695951 | 0.6423325081206411 | 9 | SIN_REFERENCIA | KEEP_FRONT_MONITOR |
| GLOBAL | 011110 | CERVEZA 33CL MAHOU 5 ESTRELLAS  NUEVA IM | ytd | 2026-YTD | A | Y | AY | 121 | 19910.16 | 97 | 3 | 12.1 | 7.803204469959762 | 0.6448929314016332 | 8 | A->A | KEEP_FRONT_MONITOR |
| GLOBAL | 012012 | CERVEZA 33CL ALHAMBRA RESERVA 1925 (5420 | ytd | 2026-YTD | A | Y | AY | 54 | 13297.08 | 32 | 4 | 5.4 | 4.673328578219169 | 0.8654312181887349 | 8 | A->A | KEEP_FRONT_MONITOR |
| GLOBAL | 014012 | CERVEZA 33CL MAHOU TOSTADA 0,0 (2751) | ytd | 2026-YTD | A | Y | AY | 53 | 4704.0 | 48 | 5 | 5.3 | 4.775981574503821 | 0.9011285989629851 | 8 | A->A | KEEP_FRONT_MONITOR |
| GLOBAL | 011127 | CERVEZA 33CL MAHOU RESERVA | ytd | 2026-YTD | A | Y | AY | 37 | 5184.0 | 26 | 6 | 3.7 | 3.034798181098704 | 0.8202157246212712 | 8 | A->A | KEEP_FRONT_MONITOR |
| GLOBAL | 124264 | ESTUCHE MAHOU ¡VUELVEN LAS CAN MAHOU TAL | ytd | 2026-YTD | A | LOW_HISTORY | ALOW_HISTORY | 36 | 36.0 | 36 | 7 | 3.6 | 9.25418824100742 | 2.5706078447242833 | 2 | B->A | KEEP_FRONT_FLEX |
| GLOBAL | 016010 | BARRIL 30L MAHOU 5 ESTRELLAS (1593) | ytd | 2026-YTD | A | Z | AZ | 35 | 203.0 | 16 | 8 | 3.5 | 5.554277630799526 | 1.5869364659427219 | 5 | A->A | KEEP_FRONT_FLEX |
| GLOBAL | 018076 | AGUA 50CL PLASTICO T.SOSTENIBLE RECICLAD | ytd | 2026-YTD | A | Z | AZ | 33 | 10064.32 | 20 | 9 | 3.3 | 4.172529209005013 | 1.2644027906075799 | 5 | A->A | KEEP_FRONT_FLEX |
| GLOBAL | 014042 | CERVEZA 33CL MAHOU SIN GLUTEN (1813) 202 | ytd | 2026-YTD | A | Z | AZ | 33 | 2448.0 | 29 | 10 | 3.3 | 3.4365680554879163 | 1.0413842592387625 | 8 | SIN_REFERENCIA | KEEP_FRONT_FLEX |
| GLOBAL | 011130 | CERVEZA MAHOU RESERVA 4X6 33CL BOTELLA C | ytd | 2026-YTD | A | Z | AZ | 31 | 3673.056 | 25 | 11 | 3.1 | 4.635730794599704 | 1.4953970305160336 | 6 | SIN_REFERENCIA | KEEP_FRONT_FLEX |
| GLOBAL | 133236 | LLAVERO MARRON BALON RUGBY (50 UNIDADES | ytd | 2026-YTD | A | Z | AZ | 29 | 3050.0 | 29 | 12 | 2.9 | 3.1128764832546763 | 1.073405683880923 | 7 | SIN_REFERENCIA | KEEP_FRONT_FLEX |
| GLOBAL | 018072 | AGUA C/GAS DE CRISTAL 33CL SOLANX24UD | ytd | 2026-YTD | A | Y | AY | 28 | 1824.0 | 27 | 13 | 2.8 | 2.4819347291981715 | 0.8864052604279185 | 8 | A->A | KEEP_FRONT_MONITOR |
| GLOBAL | 124290 | KIT FOUNDERS  RUGBY POLO NEGRO (COMPUEST | ytd | 2026-YTD | A | Z | AZ | 28 | 40.0 | 28 | 14 | 2.8 | 3.059411708155671 | 1.0926470386270253 | 6 | SIN_REFERENCIA | KEEP_FRONT_FLEX |
| GLOBAL | 092179 | BARRA CHAPA ROJA CON LED LOGO EN PASTILL | ytd | 2026-YTD | A | Z | AZ | 27 | 36.0 | 3 | 15 | 2.7 | 4.124318125460256 | 1.5275252316519465 | 3 | SIN_REFERENCIA | KEEP_FRONT_FLEX |

## Resumen ABC-XYZ del ultimo periodo global

| owner_scope | period_type | period_label | n_skus_AX | pct_pick_lines_AX | n_skus_AY | pct_pick_lines_AY | n_skus_AZ | pct_pick_lines_AZ | n_skus_BX | pct_pick_lines_BX | n_skus_BY | pct_pick_lines_BY | n_skus_BZ | pct_pick_lines_BZ | n_skus_CX | pct_pick_lines_CX | n_skus_CY | pct_pick_lines_CY | n_skus_CZ | pct_pick_lines_CZ | n_skus_ALOW_HISTORY | pct_pick_lines_ALOW_HISTORY | n_skus_BLOW_HISTORY | pct_pick_lines_BLOW_HISTORY | n_skus_CLOW_HISTORY | pct_pick_lines_CLOW_HISTORY |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| GLOBAL | ytd | 2026-YTD | 0 | 0.0 | 10 | 0.24 | 50 | 0.4063157894736842 | 0 | 0.0 | 0 | 0.0 | 0 | 0.0 | 0 | 0.0 | 0 | 0.0 | 0 | 0.0 | 78 | 0.15368421052631578 | 259 | 0.15 | 95 | 0.05 |

## Cambios relevantes entre periodos

| sku | prev_abc_class | prev_xyz_class | prev_abc_xyz_class | prev_rank | curr_abc_class | curr_xyz_class | curr_abc_xyz_class | curr_rank | owner_scope | period_type | prev_period | curr_period | class_change | abc_xyz_change | rank_delta | movement_direction |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 022154 | B | LOW_HISTORY | BLOW_HISTORY | 2353 | C | LOW_HISTORY | CLOW_HISTORY | 2401 | GLOBAL | annual | 2022 | 2023 | B->C | BLOW_HISTORY->CLOW_HISTORY | -48 | down |
| 015167 | A | LOW_HISTORY | ALOW_HISTORY | 698 | B | Z | BZ | 775 | GLOBAL | annual | 2022 | 2023 | A->B | ALOW_HISTORY->BZ | -77 | down |
| 145013 | B | LOW_HISTORY | BLOW_HISTORY | 2396 | C | LOW_HISTORY | CLOW_HISTORY | 2475 | GLOBAL | annual | 2022 | 2023 | B->C | BLOW_HISTORY->CLOW_HISTORY | -79 | down |
| 102502 | A | Z | AZ | 718 | B | Z | BZ | 827 | GLOBAL | annual | 2022 | 2023 | A->B | AZ->BZ | -109 | down |
| 103128 | A | Z | AZ | 710 | B | Z | BZ | 836 | GLOBAL | annual | 2022 | 2023 | A->B | AZ->BZ | -126 | down |
| 152020 | B | LOW_HISTORY | BLOW_HISTORY | 2403 | C | LOW_HISTORY | CLOW_HISTORY | 2548 | GLOBAL | annual | 2022 | 2023 | B->C | BLOW_HISTORY->CLOW_HISTORY | -145 | down |
| 036003 | B | LOW_HISTORY | BLOW_HISTORY | 2255 | C | LOW_HISTORY | CLOW_HISTORY | 2409 | GLOBAL | annual | 2022 | 2023 | B->C | BLOW_HISTORY->CLOW_HISTORY | -154 | down |
| 107069 | B | LOW_HISTORY | BLOW_HISTORY | 2483 | C | LOW_HISTORY | CLOW_HISTORY | 2686 | GLOBAL | annual | 2022 | 2023 | B->C | BLOW_HISTORY->CLOW_HISTORY | -203 | down |
| 207014 | A | Z | AZ | 622 | B | Z | BZ | 826 | GLOBAL | annual | 2022 | 2023 | A->B | AZ->BZ | -204 | down |
| 102214 | A | Z | AZ | 655 | B | Z | BZ | 861 | GLOBAL | annual | 2022 | 2023 | A->B | AZ->BZ | -206 | down |
| 155005 | A | Z | AZ | 587 | B | Z | BZ | 823 | GLOBAL | annual | 2022 | 2023 | A->B | AZ->BZ | -236 | down |
| 086113 | A | Z | AZ | 568 | B | Z | BZ | 805 | GLOBAL | annual | 2022 | 2023 | A->B | AZ->BZ | -237 | down |
| 124122 | A | Z | AZ | 581 | B | Z | BZ | 820 | GLOBAL | annual | 2022 | 2023 | A->B | AZ->BZ | -239 | down |
| 4900000 | A | Z | AZ | 549 | B | Z | BZ | 792 | GLOBAL | annual | 2022 | 2023 | A->B | AZ->BZ | -243 | down |
| 036009 | A | Z | AZ | 606 | B | LOW_HISTORY | BLOW_HISTORY | 850 | GLOBAL | annual | 2022 | 2023 | A->B | AZ->BLOW_HISTORY | -244 | down |
| 018020 | A | Z | AZ | 564 | B | Z | BZ | 821 | GLOBAL | annual | 2022 | 2023 | A->B | AZ->BZ | -257 | down |
| 054062 | B | LOW_HISTORY | BLOW_HISTORY | 2367 | C | LOW_HISTORY | CLOW_HISTORY | 2627 | GLOBAL | annual | 2022 | 2023 | B->C | BLOW_HISTORY->CLOW_HISTORY | -260 | down |
| 071078 | A | Z | AZ | 680 | B | LOW_HISTORY | BLOW_HISTORY | 943 | GLOBAL | annual | 2022 | 2023 | A->B | AZ->BLOW_HISTORY | -263 | down |
| 132016 | A | Z | AZ | 620 | B | LOW_HISTORY | BLOW_HISTORY | 885 | GLOBAL | annual | 2022 | 2023 | A->B | AZ->BLOW_HISTORY | -265 | down |
| 059012 | A | Z | AZ | 642 | B | LOW_HISTORY | BLOW_HISTORY | 907 | GLOBAL | annual | 2022 | 2023 | A->B | AZ->BLOW_HISTORY | -265 | down |

## Recomendaciones operativas

- `AX -> KEEP_FRONT_STABLE`: primeras posiciones y slotting muy estable.
- `AZ -> KEEP_FRONT_FLEX`: primeras posiciones, pero con buffer y flexibilidad para picos.
- `BZ -> MONITOR_VOLATILE`: revisar tendencia antes de inmovilizar espacio prime.
- `CZ -> REVIEW_SPACE`: baja prioridad; evaluar si ocupa ubicaciones demasiado valiosas.
