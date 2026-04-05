from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

import numpy as np
import pandas as pd


SCORE_SCALE_VERSION = "anchored_reference_v1"
SCORE_SCALING_METHOD = "anchored_reference_linear"

COMPARABILITY_ABSOLUTE = "absolute_comparable_inter_release"
COMPARABILITY_CONTEXTUAL = "contextual_comparable_inter_release"
COMPARABILITY_NON_COMPARABLE = "non_comparable_cross_window"

ALLOWED_COMPARABILITY_TAGS = {
    COMPARABILITY_ABSOLUTE,
    COMPARABILITY_CONTEXTUAL,
    COMPARABILITY_NON_COMPARABLE,
}


@dataclass(frozen=True)
class AnchoredBand:
    low: float
    high: float
    invert: bool = False


def anchored_score(
    series: pd.Series,
    *,
    low: float,
    high: float,
    invert: bool = False,
    clip_range: tuple[float, float] = (0.0, 100.0),
) -> pd.Series:
    values = series.astype(float)
    span = float(high) - float(low)
    if not np.isfinite(span) or span <= 0:
        return pd.Series(np.ones(len(values)) * 50.0, index=values.index)

    score = 100.0 * (values - float(low)) / span
    if invert:
        score = 100.0 - score
    return score.clip(clip_range[0], clip_range[1])


def anchored_score_by_group(
    values: pd.Series,
    groups: pd.Series,
    *,
    bands: Mapping[str, AnchoredBand],
    default_band: AnchoredBand,
    clip_range: tuple[float, float] = (0.0, 100.0),
) -> pd.Series:
    out = pd.Series(np.nan, index=values.index, dtype=float)
    group_labels = groups.astype(str)

    for group_name in group_labels.dropna().unique():
        band = bands.get(str(group_name), default_band)
        mask = group_labels == group_name
        out.loc[mask] = anchored_score(
            values.loc[mask],
            low=band.low,
            high=band.high,
            invert=band.invert,
            clip_range=clip_range,
        )

    missing_mask = out.isna()
    if missing_mask.any():
        out.loc[missing_mask] = anchored_score(
            values.loc[missing_mask],
            low=default_band.low,
            high=default_band.high,
            invert=default_band.invert,
            clip_range=clip_range,
        )
    return out


def legacy_local_minmax_score(series: pd.Series, *, invert: bool = False) -> pd.Series:
    values = series.astype(float)
    v_min = values.min()
    v_max = values.max()
    if pd.isna(v_min) or pd.isna(v_max) or v_min == v_max:
        out = pd.Series(np.ones(len(values)) * 50.0, index=values.index)
    else:
        out = 100.0 * (values - v_min) / (v_max - v_min)
    if invert:
        out = 100.0 - out
    return out.clip(0.0, 100.0)


def topk_overlap(base_scores: pd.Series, perturbed_scores: pd.Series, ids: pd.Series, *, topk: int = 10) -> int:
    base_top = (
        pd.DataFrame({"id": ids.astype(str), "score": base_scores.astype(float)})
        .sort_values("score", ascending=False)
        .head(topk)["id"]
        .tolist()
    )
    pert_top = (
        pd.DataFrame({"id": ids.astype(str), "score": perturbed_scores.astype(float)})
        .sort_values("score", ascending=False)
        .head(topk)["id"]
        .tolist()
    )
    return len(set(base_top) & set(pert_top))


def outlier_impact_median_abs_delta(
    raw_series: pd.Series,
    score_fn: callable,
    *,
    outlier_multiplier: float = 3.0,
) -> float:
    raw = raw_series.astype(float).copy()
    base_score = score_fn(raw)

    if len(raw) == 0:
        return 0.0

    idx = raw.idxmax()
    pert = raw.copy()
    pert.loc[idx] = pert.loc[idx] * float(outlier_multiplier)
    pert_score = score_fn(pert)
    delta = (pert_score - base_score).abs()
    if len(delta) <= 1:
        return float(delta.mean())
    return float(delta.drop(index=idx, errors="ignore").median())


def spearman_rank_corr(a: pd.Series, b: pd.Series) -> float:
    aa = a.astype(float)
    bb = b.astype(float)
    ra = aa.rank(method="average")
    rb = bb.rank(method="average")
    if float(ra.std(ddof=0)) <= 1e-12 or float(rb.std(ddof=0)) <= 1e-12:
        return 0.0
    corr = ra.corr(rb, method="pearson")
    if pd.isna(corr):
        return 0.0
    return float(corr)
