from __future__ import annotations

import math
from typing import Tuple


def _to_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).upper()


def _contains(text: str, token: str) -> bool:
    return token in text if text else False


def classify_service_type(
    codigo_norm: str | None,
    concepto: str | None,
    m3_in: float | int | None,
    m3_out: float | int | None,
) -> Tuple[str, str]:
    code = _to_text(codigo_norm)
    concept = _to_text(concepto)

    # Regla 1: prefijos de codigo.
    if code.startswith(("SGE", "SGP")):
        return "entrega", "prefijo_codigo"
    if code.startswith(("EGE", "EGP")):
        return "recogida", "prefijo_codigo"

    # Regla 2: concepto exacto esperado.
    if _contains(concept, "PORTE ENTREGA") or _contains(concept, "PORTE ENVIO"):
        return "entrega", "concepto_porte"
    if _contains(concept, "PORTE RECOGIDA"):
        return "recogida", "concepto_porte"

    # Regla 3: keywords ampliadas.
    if _contains(concept, "RECOGIDA"):
        return "recogida", "concepto_keyword"
    if (
        _contains(concept, "ENTREGA")
        or _contains(concept, "EXPEDICION")
        or _contains(concept, "ENVIO")
    ):
        return "entrega", "concepto_keyword"

    # Regla 4: fallback por IN/OUT.
    m3_in_v = float(m3_in) if m3_in is not None and not (isinstance(m3_in, float) and math.isnan(m3_in)) else 0.0
    m3_out_v = float(m3_out) if m3_out is not None and not (isinstance(m3_out, float) and math.isnan(m3_out)) else 0.0

    if m3_out_v > 0 and m3_in_v <= 0:
        return "entrega", "fallback_in_out"
    if m3_in_v > 0 and m3_out_v <= 0:
        return "recogida", "fallback_in_out"
    if m3_in_v > 0 and m3_out_v > 0:
        return "mixto", "fallback_in_out"
    return "desconocida", "fallback_desconocida"
