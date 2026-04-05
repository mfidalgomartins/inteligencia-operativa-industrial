# Investment Prioritization Framework

## Posicionamiento de madurez financiera
- Screening financiero: comparabilidad temprana de iniciativas.
- Pre-feasibility: discounted value/downside/cost of delay con supuestos explícitos.
- Business case candidate: requiere validación corporativa adicional (no final).

## Taxonomía financiera activa
- gross_technical_value
- avoided_loss
- net_captured_value
- discounted_value
- downside_adjusted_value
- cost_of_delay_12m
- capital_efficiency
- screening_irr_pct
- formal_irr_candidate_pct
- screening_var_95_npv

## Regla de credibilidad
- No presentar `screening_irr_pct` como IRR formal de comité.
- No presentar `business_case_candidate` como FID final aprobado.
- Scoring gobernado con `anchored_reference_linear` y versión `anchored_reference_v1`.

## Motor de selección de cartera
- Modelo binario branch-and-bound con restricciones explícitas.
- Dependencias y exclusiones técnicas tratadas como restricciones hard.
- Mandatory/compliance forzadas y cobertura mínima reliability en formulación.
- Estado de resolución actual: solved_exact.
- Grado de optimalidad declarado: exact_for_formulation.