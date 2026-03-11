# ABC Picking

Analisis Pareto / clasificacion ABC de picking basado en `pick_lines` (numero de movimientos PI por SKU).

## Regla principal

- La clasificacion ABC se calcula por `pick_lines`, no por unidades.
- Las metricas secundarias (`pick_qty`, `n_orders`, `n_days_active`) son auxiliares para layout y slotting.

## Cobertura del analisis

- Lineas PI validas con fecha: 65189
- SKUs analizados: 7795
- Rango de fechas: 2022-01-03 00:00:00 -> 2026-03-05 00:00:00
- Registros descartados por fecha invalida: 0

## Como usarlo para layout

- `A`: SKUs de mayor concentracion operativa. Son candidatos a posiciones frontales o zonas calientes.
- `B`: mantener accesibles, pero sin consumir ubicaciones premium si no hay restriccion operativa.
- `C`: baja prioridad; revisar si ocupan espacio prime de forma innecesaria.

Ultimo periodo disponible: `2026-YTD`
Concentracion de pick_lines en clase A: `80.0%`

## Candidatos de layout (ultimo periodo)

| sku | denominacion | latest_period_type | latest_period | latest_abc_class | latest_pick_lines | latest_pick_qty | latest_n_orders | latest_rank | change_vs_prev_period | recommendation_tag |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 019080 | TINTO DE VERANO CON LIMON CACHIS MARIANO | ytd | 2026-YTD | A | 223 | 8052.0 | 219 | 1 | SIN_REFERENCIA | KEEP_FRONT |
| 018428 | AGUA 50CL PLÁSTICO SOLAN 2025 CALCIO, MA | ytd | 2026-YTD | A | 124 | 27531.7 | 83 | 2 | SIN_REFERENCIA | KEEP_FRONT |
| 011110 | CERVEZA 33CL MAHOU 5 ESTRELLAS  NUEVA IM | ytd | 2026-YTD | A | 121 | 19910.16 | 97 | 3 | A->A | KEEP_FRONT |
| 012012 | CERVEZA 33CL ALHAMBRA RESERVA 1925 (5420 | ytd | 2026-YTD | A | 54 | 13297.08 | 32 | 4 | A->A | KEEP_FRONT |
| 014012 | CERVEZA 33CL MAHOU TOSTADA 0,0 (2751) | ytd | 2026-YTD | A | 53 | 4704.0 | 48 | 5 | A->A | KEEP_FRONT |
| 011127 | CERVEZA 33CL MAHOU RESERVA | ytd | 2026-YTD | A | 37 | 5184.0 | 26 | 6 | A->A | KEEP_FRONT |
| 124264 | ESTUCHE MAHOU ¡VUELVEN LAS CAN MAHOU TAL | ytd | 2026-YTD | A | 36 | 36.0 | 36 | 7 | B->A | REVIEW_UPGRADE |
| 016010 | BARRIL 30L MAHOU 5 ESTRELLAS (1593) | ytd | 2026-YTD | A | 35 | 203.0 | 16 | 8 | A->A | KEEP_FRONT |
| 018076 | AGUA 50CL PLASTICO T.SOSTENIBLE RECICLAD | ytd | 2026-YTD | A | 33 | 10064.32 | 20 | 9 | A->A | KEEP_FRONT |
| 014042 | CERVEZA 33CL MAHOU SIN GLUTEN (1813) 202 | ytd | 2026-YTD | A | 33 | 2448.0 | 29 | 10 | SIN_REFERENCIA | KEEP_FRONT |
| 011130 | CERVEZA MAHOU RESERVA 4X6 33CL BOTELLA C | ytd | 2026-YTD | A | 31 | 3673.056 | 25 | 11 | SIN_REFERENCIA | KEEP_FRONT |
| 133236 | LLAVERO MARRON BALON RUGBY (50 UNIDADES | ytd | 2026-YTD | A | 29 | 3050.0 | 29 | 12 | SIN_REFERENCIA | KEEP_FRONT |
| 018072 | AGUA C/GAS DE CRISTAL 33CL SOLANX24UD | ytd | 2026-YTD | A | 28 | 1824.0 | 27 | 13 | A->A | KEEP_FRONT |
| 124290 | KIT FOUNDERS  RUGBY POLO NEGRO (COMPUEST | ytd | 2026-YTD | A | 28 | 40.0 | 28 | 14 | SIN_REFERENCIA | KEEP_FRONT |
| 092179 | BARRA CHAPA ROJA CON LED LOGO EN PASTILL | ytd | 2026-YTD | A | 27 | 36.0 | 3 | 15 | SIN_REFERENCIA | KEEP_FRONT |

## Cambios relevantes entre periodos

| sku | prev_abc_class | prev_rank | curr_abc_class | curr_rank | period_type | prev_period | curr_period | class_change | rank_delta | movement_direction |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 022154 | B | 2353 | C | 2401 | annual | 2022 | 2023 | B->C | -48 | down |
| 015167 | A | 698 | B | 775 | annual | 2022 | 2023 | A->B | -77 | down |
| 145013 | B | 2396 | C | 2475 | annual | 2022 | 2023 | B->C | -79 | down |
| 102502 | A | 718 | B | 827 | annual | 2022 | 2023 | A->B | -109 | down |
| 103128 | A | 710 | B | 836 | annual | 2022 | 2023 | A->B | -126 | down |
| 152020 | B | 2403 | C | 2548 | annual | 2022 | 2023 | B->C | -145 | down |
| 036003 | B | 2255 | C | 2409 | annual | 2022 | 2023 | B->C | -154 | down |
| 107069 | B | 2483 | C | 2686 | annual | 2022 | 2023 | B->C | -203 | down |
| 207014 | A | 622 | B | 826 | annual | 2022 | 2023 | A->B | -204 | down |
| 102214 | A | 655 | B | 861 | annual | 2022 | 2023 | A->B | -206 | down |
| 155005 | A | 587 | B | 823 | annual | 2022 | 2023 | A->B | -236 | down |
| 086113 | A | 568 | B | 805 | annual | 2022 | 2023 | A->B | -237 | down |
| 124122 | A | 581 | B | 820 | annual | 2022 | 2023 | A->B | -239 | down |
| 4900000 | A | 549 | B | 792 | annual | 2022 | 2023 | A->B | -243 | down |
| 036009 | A | 606 | B | 850 | annual | 2022 | 2023 | A->B | -244 | down |
| 018020 | A | 564 | B | 821 | annual | 2022 | 2023 | A->B | -257 | down |
| 054062 | B | 2367 | C | 2627 | annual | 2022 | 2023 | B->C | -260 | down |
| 071078 | A | 680 | B | 943 | annual | 2022 | 2023 | A->B | -263 | down |
| 132016 | A | 620 | B | 885 | annual | 2022 | 2023 | A->B | -265 | down |
| 059012 | A | 642 | B | 907 | annual | 2022 | 2023 | A->B | -265 | down |
