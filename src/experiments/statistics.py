"""Independent-run statistics. No Docker or Prometheus dependencies."""

import hashlib
from itertools import combinations
import math

import numpy as np
from scipy.stats import mannwhitneyu


BOOTSTRAP_SAMPLES = 10_000


def rng_for(seed: int, key: str):
    digest = hashlib.sha256(f"{seed}:{key}".encode()).digest()
    return np.random.default_rng(int.from_bytes(digest[:16], "big"))


def effect_category(delta: float) -> str:
    value = abs(delta)
    return ("negligible" if value < .147 else "small" if value < .33
            else "medium" if value < .474 else "large")


def cliffs_delta(a, b) -> float:
    # searchsorted avoids a quadratic comparison matrix for large repeat counts.
    a, b = np.asarray(a), np.sort(b)
    return float(np.sum(np.searchsorted(b, a, side="left") -
                        (len(b) - np.searchsorted(b, a, side="right"))) / (len(a) * len(b)))


def describe(values, rng, resamples=BOOTSTRAP_SAMPLES):
    x = np.asarray(values, dtype=float)
    if not len(x):
        return dict(n=0, mean=None, median=None, q1=None, q3=None, iqr=None,
                    mean_ci95=None, median_ci95=None, small_sample=True)
    q1, median, q3 = np.quantile(x, [.25, .5, .75], method="linear")
    result = dict(n=len(x), mean=float(x.mean()), median=float(median),
                  q1=float(q1), q3=float(q3), iqr=float(q3-q1),
                  mean_ci95=None, median_ci95=None, small_sample=len(x) < 5)
    if len(x) > 1 and np.all(x == x[0]):
        result.update(mean_ci95=[float(x[0])]*2, median_ci95=[float(x[0])]*2)
    elif len(x) > 1:
        means, medians = [], []
        for offset in range(0, resamples, 128):
            draw = rng.choice(x, size=(min(128, resamples-offset), len(x)))
            means.extend(draw.mean(axis=1))
            medians.extend(np.median(draw, axis=1))
        result.update(mean_ci95=np.quantile(means, [.025, .975]).tolist(),
                      median_ci95=np.quantile(medians, [.025, .975]).tolist())
    return result


def compare(a, b, rng, resamples=BOOTSTRAP_SAMPLES):
    result = dict(n_a=len(a), n_b=len(b), test="mann_whitney_u", alternative="two-sided",
                  p_value=None, p_adjusted=None, cliffs_delta=None, delta_ci95=None,
                  effect_category=None, small_sample=min(len(a), len(b)) < 5)
    if min(len(a), len(b)) < 2:
        return dict(result, reason="at_least_two_valid_runs_per_group_required")
    joined = np.concatenate([a, b])
    method = "exact" if min(len(a), len(b)) <= 8 and len(np.unique(joined)) == len(joined) else "asymptotic"
    constant = np.all(joined == joined[0])
    test = None if constant else mannwhitneyu(a, b, alternative="two-sided", method=method)
    delta = cliffs_delta(a, b)
    if constant:
        interval = [0.0, 0.0]
    else:
        a, b = np.asarray(a), np.asarray(b)
        signs = (a[:, None] > b[None, :]).astype(float) - (a[:, None] < b[None, :])
        draws = []
        for offset in range(0, resamples, 128):
            count = min(128, resamples-offset)
            wa = rng.multinomial(len(a), np.full(len(a), 1/len(a)), count)
            wb = rng.multinomial(len(b), np.full(len(b), 1/len(b)), count)
            draws.extend(np.einsum("bi,ij,bj->b", wa, signs, wb, optimize=True)/(len(a)*len(b)))
        interval = np.quantile(draws, [.025, .975]).tolist()
    return dict(result, method=method, u_statistic=len(a)*len(b)/2 if constant else float(test.statistic),
                p_value=1.0 if constant else float(test.pvalue), cliffs_delta=delta,
                delta_ci95=interval,
                effect_category=effect_category(delta))


def holm(p_values):
    order = sorted(range(len(p_values)), key=p_values.__getitem__)
    adjusted = [0.0] * len(order)
    maximum = 0.0
    for rank, index in enumerate(order):
        maximum = max(maximum, (len(order)-rank) * p_values[index])
        adjusted[index] = min(1.0, maximum)
    return adjusted


def flatten_metrics(value, prefix=""):
    result = {}
    for key, item in value.items():
        name = f"{prefix}.{key}" if prefix else key
        if isinstance(item, dict):
            result.update(flatten_metrics(item, name))
        elif isinstance(item, (float, int)) and not isinstance(item, bool) and math.isfinite(item):
            result[name] = float(item)
    return result


def analyze(spec: dict, plan: dict, records: list[dict]):
    """Records include failed/missing runs, so exclusions remain visible."""
    configurations = {c["configuration_id"]: c for c in plan["configurations"]}
    metric_names = {"latency_seconds.average", "latency_seconds.p50", "latency_seconds.p75",
                    "latency_seconds.p95", "latency_seconds.p99", "rates.producer_messages_per_second",
                    "rates.consumer_messages_per_second", "rates.producer_payload_bytes_per_second"}
    for role in ("broker", "producer", "consumer"):
        metric_names.update(f"resources.{role}.{name}" for name in
                            ("cpu_cores_average", "cpu_cores_maximum", "memory_bytes_average", "memory_bytes_maximum"))
    rows = []
    for record in records:
        artifact = record.get("artifact") or {}
        metrics = artifact.get("metrics", {})
        flat = flatten_metrics({k: v for k, v in metrics.items()
                                if k in ("latency_seconds", "rates", "resources", "messages", "reliability", "steps")})
        # Steps are a list in the raw artifact; each step forms its own metric family.
        for i, step in enumerate(metrics.get("steps", [])):
            flat.update(flatten_metrics(step.get("metrics", {}), f"step_{i}"))
        quality = metrics.get("quality", {})
        reasons = quality.get("excluded_metrics", {})
        metric_names.update(reasons)
        valid = record["status"] == "finished" and artifact.get("scenario", {}).get("status") == "FINISHED"
        row = dict(record, artifact=None, metrics=flat, exclusions=dict(reasons))
        if not valid:
            row["invalid_reason"] = record.get("error") or "run_not_finished"
        elif not quality.get("final_metrics_collected", False):
            row["invalid_reason"] = "final_scrape_not_confirmed"
        rows.append(row)
        metric_names.update(flat)
    groups, comparisons = [], []
    samples = {}
    for cid, config in configurations.items():
        group_rows = [r for r in rows if r["configuration_id"] == cid]
        statistics = {}
        for metric in sorted(metric_names):
            values, excluded = [], []
            for row in group_rows:
                reason = row.get("invalid_reason") or row["exclusions"].get(metric)
                number = row["metrics"].get(metric)
                if reason or number is None:
                    excluded.append(dict(run_id=row["run_id"], reason=reason or "metric_missing"))
                else:
                    values.append(number)
            samples[cid, metric] = values
            statistics[metric] = dict(describe(values, rng_for(spec["seed"], f"{cid}:{metric}")),
                                      excluded=excluded, excluded_count=len(excluded))
        groups.append(dict(configuration_id=cid, broker=config["broker"], value=config["value"],
                           expected_repetitions=spec["repetitions"], metrics=statistics))
    for a, b in combinations(configurations.values(), 2):
        if a["value"] != b["value"] or a["broker"] == b["broker"]:
            continue
        for metric in sorted(metric_names):
            key = f"{a['configuration_id']}:{b['configuration_id']}:{metric}"
            comparisons.append(dict(value=a["value"], broker_a=a["broker"], broker_b=b["broker"],
                                    metric=metric, **compare(samples[a["configuration_id"], metric],
                                                           samples[b["configuration_id"], metric],
                                                           rng_for(spec["seed"], key))))
    for metric in metric_names:
        family = [c for c in comparisons if c["metric"] == metric and c["p_value"] is not None]
        for comparison, adjusted in zip(family, holm([c["p_value"] for c in family])):
            comparison.update(p_adjusted=adjusted, significant=adjusted < .05,
                              correction="holm", family_size=len(family))
    incomplete = [dict(configuration_id=g["configuration_id"],
                       metrics=[name for name, stat in g["metrics"].items() if stat["n"] < spec["repetitions"]])
                  for g in groups]
    incomplete = [g for g in incomplete if g["metrics"]]
    return (dict(schema_version=1, groups=groups, bootstrap_resamples=BOOTSTRAP_SAMPLES,
                 valid_run_count=sum(not row.get("invalid_reason") for row in rows),
                 total_run_count=len(rows),
                 incomplete_groups=incomplete,
                 confidence_level=.95, intervals="individual_percentile", seed=spec["seed"],
                 independent_unit="run"),
            dict(schema_version=1, comparisons=comparisons, alpha=.05,
                 correction_family="experiment_and_metric_across_all_values"), rows)
