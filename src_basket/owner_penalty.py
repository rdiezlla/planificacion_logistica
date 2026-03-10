from __future__ import annotations

import pandas as pd


def build_owner_penalty(lines: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    per_order = (
        lines.groupby("pedido_id", dropna=False)
        .agg(
            n_propietarios_distintos=("propietario_norm", "nunique"),
            n_lineas_pi_totales=("sku", "size"),
            n_skus_distintos_totales=("sku", "nunique"),
        )
        .reset_index()
    )
    per_order["proxy_recorridos"] = per_order["n_propietarios_distintos"].clip(lower=1)
    per_order["penalty_ratio"] = per_order["proxy_recorridos"].astype(float)
    per_order["is_multi_propietario"] = per_order["n_propietarios_distintos"].gt(1).astype(int)

    multi_orders = per_order["is_multi_propietario"].eq(1)
    kpis = pd.DataFrame(
        [
            {
                "pct_pedidos_multi_propietario": float(multi_orders.mean()) if len(per_order) else 0.0,
                "pct_lineas_pi_en_multi_propietario": float(
                    per_order.loc[multi_orders, "n_lineas_pi_totales"].sum()
                    / max(per_order["n_lineas_pi_totales"].sum(), 1)
                ),
                "duplicacion_media_recorridos": float(per_order["proxy_recorridos"].mean()) if len(per_order) else 0.0,
                "top20_pedidos_multi_max_propietarios": "|".join(
                    per_order.sort_values(
                        ["n_propietarios_distintos", "n_lineas_pi_totales", "pedido_id"],
                        ascending=[False, False, True],
                    )
                    .head(20)["pedido_id"]
                    .astype(str)
                    .tolist()
                ),
            }
        ]
    )
    return per_order.sort_values(
        ["n_propietarios_distintos", "n_lineas_pi_totales", "pedido_id"],
        ascending=[False, False, True],
    ).reset_index(drop=True), kpis
