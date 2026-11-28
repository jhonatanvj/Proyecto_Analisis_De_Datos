"""Storytelling summary: aggregations behind the functional-user dashboard (Workshop 3, Part 3).

build_storytelling() receives the Silver topics read by silver_reader and returns rows in the Gold
contract (see contract.py). Each aggregation maps to a user story from Workshop 1 (US1-US6)
and feeds a specific chart or KPI card in the Workshop 4 dashboard:

* earthquakes_weekly: weekly earthquake counts near US volcanoes (US2/US3).
* tone_weekly: weekly volcanic-lexicon vs. VADER activity tone and official alert level
  trajectory across the HANS notice corpus (building vs. fading unrest).
* top_keywords_bigrams: top unigrams and bigrams in preprocessed notice summaries and their
  mean activity tone.
* api_vs_scraping_comparison: volume, text length, and tone comparison between official HANS
  API notices and scraped USGS Volcano Updates.
* earthquakes_vs_baseline: latest weekly earthquake counts per volcano compared against each
  volcano's 2010-present historical weekly baseline and p90 threshold (US3).
* alerts_by_observatory_region: current USGS alert level and aviation colour code breakdown by
  observatory and region (US1, US6).
"""
from __future__ import annotations

from datetime import datetime


def earthquakes_weekly(spark, silver: dict):
    from pyspark.sql import functions as F
    from .silver_reader import latest

    eq = silver.get("earthquakes")
    if eq is None:
        return None

    return (
        latest(eq)
        .where(F.col("nearest_vnum").isNotNull())
        .groupBy(F.date_trunc("week", "event_time").alias("period"))
        .agg(F.count("*").cast("double").alias("value"))
        .select(
            F.lit("earthquakes_weekly").alias("section"),
            F.lit("earthquakes").alias("metric"),
            "period",
            "value",
            F.lit("records").alias("unit"),
        )
    )


def tone_weekly(spark, silver: dict):
    """Weekly activity tone (domain lexicon vs. VADER) vs. official alert level (building/fading)."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from .silver_reader import latest
    from .tone import lexicon_score, vader_score

    notices = silver.get("notices")
    if notices is None:
        return None

    lex_udf = F.udf(lexicon_score, T.DoubleType())
    vad_udf = F.udf(vader_score, T.DoubleType())

    # Deduplicate to one notice per volcano per week (preferring WU/SR/VAN over repetitive DU)
    # while keeping an overall weekly trajectory across all US volcanoes.
    df = (
        latest(notices)
        .where(F.col("sent_time").isNotNull() & F.col("summary_clean").isNotNull())
        .withColumn("period", F.date_trunc("week", "sent_time"))
        .withColumn("lex_tone", lex_udf(F.col("summary_clean")))
        .withColumn("vader_tone", vad_udf(F.col("summary_clean")))
    )

    weekly_all = df.groupBy("period").agg(
        F.avg("lex_tone").alias("lexicon_mean"),
        F.avg("vader_tone").alias("vader_mean"),
        F.avg(F.col("alert_level_ord").cast("double")).alias("alert_ord_mean"),
        F.count("*").cast("double").alias("notice_count"),
    )

    out_lex = weekly_all.select(
        F.lit("tone_weekly").alias("section"),
        F.lit("lexicon_mean").alias("metric"),
        F.lit("ALL").alias("dimension"),
        "period",
        F.col("lexicon_mean").alias("value"),
        F.lit("score").alias("unit"),
    )
    out_vad = weekly_all.select(
        F.lit("tone_weekly").alias("section"),
        F.lit("vader_mean").alias("metric"),
        F.lit("ALL").alias("dimension"),
        "period",
        F.col("vader_mean").alias("value"),
        F.lit("score").alias("unit"),
    )
    out_alert = weekly_all.where(F.col("alert_ord_mean").isNotNull()).select(
        F.lit("tone_weekly").alias("section"),
        F.lit("alert_level_mean").alias("metric"),
        F.lit("ALL").alias("dimension"),
        "period",
        F.col("alert_ord_mean").alias("value"),
        F.lit("ordinal_1_4").alias("unit"),
    )

    # Also compute per-volcano weekly lexicon tone for volcanoes with elevated alerts
    elevated_vnums = (
        df.where(F.col("alert_level_ord") >= 2)
        .select("vnum")
        .distinct()
    )
    weekly_volc = (
        df.join(elevated_vnums, on="vnum", how="inner")
        .groupBy("period", "vnum")
        .agg(F.avg("lex_tone").alias("value"))
        .select(
            F.lit("tone_weekly").alias("section"),
            F.lit("lexicon_mean").alias("metric"),
            F.col("vnum").cast("string").alias("dimension"),
            "period",
            "value",
            F.lit("score").alias("unit"),
        )
    )

    return out_lex.unionByName(out_vad).unionByName(out_alert).unionByName(weekly_volc)


def top_keywords_bigrams(spark, silver: dict):
    """Top 25 unigrams and top 20 bigrams from preprocessed notice summaries + mean tone."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from .silver_reader import latest
    from .tone import lexicon_score

    notices = silver.get("notices")
    if notices is None or "summary_tokens" not in notices.columns:
        return None

    lex_udf = F.udf(lexicon_score, T.DoubleType())
    base = (
        latest(notices)
        .where(F.col("summary_tokens").isNotNull() & (F.size("summary_tokens") > 0))
        .withColumn("lex_tone", lex_udf(F.col("summary_clean")))
    )

    # Unigrams
    uni = (
        base.select(F.explode("summary_tokens").alias("term"), "lex_tone")
        .where(F.length("term") >= 3)
        .groupBy("term")
        .agg(
            F.count("*").cast("double").alias("freq"),
            F.avg("lex_tone").alias("mean_tone"),
        )
        .orderBy(F.desc("freq"))
        .limit(25)
    )
    uni_freq = uni.select(
        F.lit("top_keywords").alias("section"),
        F.lit("frequency").alias("metric"),
        F.concat(F.lit("unigram:"), F.col("term")).alias("dimension"),
        F.col("freq").alias("value"),
        F.lit("count").alias("unit"),
    )
    uni_tone = uni.select(
        F.lit("top_keywords").alias("section"),
        F.lit("mean_lexicon_tone").alias("metric"),
        F.concat(F.lit("unigram:"), F.col("term")).alias("dimension"),
        F.col("mean_tone").alias("value"),
        F.lit("score").alias("unit"),
    )

    # Bigrams via adjacent array zip
    bi_arrays = base.select(
        F.expr(
            "transform(sequence(0, size(summary_tokens) - 2), "
            "i -> concat(summary_tokens[i], ' ', summary_tokens[i + 1]))"
        ).alias("bigrams"),
        "lex_tone",
    )
    bi = (
        bi_arrays.select(F.explode("bigrams").alias("term"), "lex_tone")
        .groupBy("term")
        .agg(
            F.count("*").cast("double").alias("freq"),
            F.avg("lex_tone").alias("mean_tone"),
        )
        .orderBy(F.desc("freq"))
        .limit(20)
    )
    bi_freq = bi.select(
        F.lit("top_keywords").alias("section"),
        F.lit("frequency").alias("metric"),
        F.concat(F.lit("bigram:"), F.col("term")).alias("dimension"),
        F.col("freq").alias("value"),
        F.lit("count").alias("unit"),
    )
    bi_tone = bi.select(
        F.lit("top_keywords").alias("section"),
        F.lit("mean_lexicon_tone").alias("metric"),
        F.concat(F.lit("bigram:"), F.col("term")).alias("dimension"),
        F.col("mean_tone").alias("value"),
        F.lit("score").alias("unit"),
    )

    return uni_freq.unionByName(uni_tone).unionByName(bi_freq).unionByName(bi_tone)


def api_vs_scraping_comparison(spark, silver: dict):
    """Compare volume, text length, and activity tone between HANS API and scraped updates."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from .silver_reader import latest
    from .tone import lexicon_score, vader_score

    notices = silver.get("notices")
    updates = silver.get("volcano_updates")
    if notices is None and updates is None:
        return None

    lex_udf = F.udf(lexicon_score, T.DoubleType())
    vad_udf = F.udf(vader_score, T.DoubleType())

    frames = []
    if notices is not None:
        n_df = (
            latest(notices)
            .where(F.col("text_clean").isNotNull())
            .withColumn("lex", lex_udf(F.col("text_clean")))
            .withColumn("vad", vad_udf(F.col("text_clean")))
            .agg(
                F.count("*").cast("double").alias("records"),
                F.avg(F.length("text_clean").cast("double")).alias("mean_chars"),
                F.avg("lex").alias("lexicon_mean"),
                F.avg("vad").alias("vader_mean"),
            )
            .withColumn("source", F.lit("usgs_hans_notices_api"))
        )
        frames.append(n_df)

    if updates is not None:
        u_df = (
            latest(updates)
            .where(F.col("text_clean").isNotNull())
            .withColumn("lex", lex_udf(F.col("text_clean")))
            .withColumn("vad", vad_udf(F.col("text_clean")))
            .agg(
                F.count("*").cast("double").alias("records"),
                F.avg(F.length("text_clean").cast("double")).alias("mean_chars"),
                F.avg("lex").alias("lexicon_mean"),
                F.avg("vad").alias("vader_mean"),
            )
            .withColumn("source", F.lit("usgs_volcano_updates_scraping"))
        )
        frames.append(u_df)

    combined = frames[0] if len(frames) == 1 else frames[0].unionByName(frames[1])

    m_rec = combined.select(
        F.lit("source_comparison").alias("section"),
        F.lit("records").alias("metric"),
        F.col("source").alias("dimension"),
        F.col("records").alias("value"),
        F.lit("records").alias("unit"),
    )
    m_len = combined.select(
        F.lit("source_comparison").alias("section"),
        F.lit("mean_text_length").alias("metric"),
        F.col("source").alias("dimension"),
        F.col("mean_chars").alias("value"),
        F.lit("characters").alias("unit"),
    )
    m_lex = combined.select(
        F.lit("source_comparison").alias("section"),
        F.lit("lexicon_mean").alias("metric"),
        F.col("source").alias("dimension"),
        F.col("lexicon_mean").alias("value"),
        F.lit("score").alias("unit"),
    )
    m_vad = combined.select(
        F.lit("source_comparison").alias("section"),
        F.lit("vader_mean").alias("metric"),
        F.col("source").alias("dimension"),
        F.col("vader_mean").alias("value"),
        F.lit("score").alias("unit"),
    )
    return m_rec.unionByName(m_len).unionByName(m_lex).unionByName(m_vad)


def earthquakes_vs_baseline(spark, silver: dict):
    """US3: Weekly earthquake count per volcano compared against its historical baseline (p50/p90)."""
    from pyspark.sql import functions as F
    from .silver_reader import latest

    eq = silver.get("earthquakes")
    if eq is None:
        return None

    weekly = (
        latest(eq)
        .where(F.col("nearest_vnum").isNotNull() & F.col("event_time").isNotNull())
        .groupBy(
            F.col("nearest_vnum").cast("string").alias("vnum"),
            F.date_trunc("week", "event_time").alias("period"),
        )
        .agg(F.count("*").cast("double").alias("weekly_count"))
    )

    baseline = weekly.groupBy("vnum").agg(
        F.avg("weekly_count").alias("baseline_mean"),
        F.expr("percentile_approx(weekly_count, 0.9)").alias("baseline_p90"),
    )

    # Keep the latest 8 weeks per volcano so the Gold file stays compact for Dash
    max_period_row = weekly.agg(F.max("period").alias("max_p")).first()
    if max_period_row is None or max_period_row["max_p"] is None:
        return None

    recent = (
        weekly.where(F.col("period") >= F.date_sub(F.lit(max_period_row["max_p"]), 56))
        .join(baseline, on="vnum", how="inner")
        .withColumn(
            "ratio_to_p90",
            F.when(F.col("baseline_p90") > 0, F.col("weekly_count") / F.col("baseline_p90"))
            .otherwise(F.col("weekly_count")),
        )
    )

    out_cnt = recent.select(
        F.lit("earthquakes_baseline").alias("section"),
        F.lit("weekly_count").alias("metric"),
        F.col("vnum").alias("dimension"),
        "period",
        F.col("weekly_count").alias("value"),
        F.lit("earthquakes").alias("unit"),
    )
    out_p90 = recent.select(
        F.lit("earthquakes_baseline").alias("section"),
        F.lit("baseline_p90").alias("metric"),
        F.col("vnum").alias("dimension"),
        "period",
        F.col("baseline_p90").alias("value"),
        F.lit("earthquakes").alias("unit"),
    )
    out_ratio = recent.select(
        F.lit("earthquakes_baseline").alias("section"),
        F.lit("baseline_ratio").alias("metric"),
        F.col("vnum").alias("dimension"),
        "period",
        F.col("ratio_to_p90").alias("value"),
        F.lit("ratio").alias("unit"),
    )
    return out_cnt.unionByName(out_p90).unionByName(out_ratio)


def alerts_by_observatory_region(spark, silver: dict):
    """US1/US6: Current alert level and aviation colour code breakdown by observatory and region."""
    from pyspark.sql import functions as F
    from .silver_reader import latest

    alerts = silver.get("alert_notices")
    notices = silver.get("notices")
    if alerts is None:
        return None

    al = latest(alerts).select(
        "vnum",
        F.coalesce(F.col("alert_level"), F.lit("UNKNOWN")).alias("alert_level"),
        F.coalesce(F.col("color_code"), F.lit("UNKNOWN")).alias("color_code"),
    )

    # Enrich alert_notices with observatory and region from the latest notice per volcano
    if notices is not None and "observatory" in notices.columns and "region" in notices.columns:
        meta = (
            latest(notices)
            .where(F.col("vnum").isNotNull())
            .groupBy("vnum")
            .agg(
                F.first("observatory", ignorenulls=True).alias("observatory"),
                F.first("region", ignorenulls=True).alias("region"),
            )
        )
        enriched = al.join(meta, on="vnum", how="left")
    else:
        enriched = al.withColumn("observatory", F.lit("UNKNOWN")).withColumn("region", F.lit("UNKNOWN"))

    enriched = enriched.fillna({"observatory": "UNKNOWN", "region": "UNKNOWN"})

    by_obs = (
        enriched.groupBy("observatory", "alert_level")
        .agg(F.countDistinct("vnum").cast("double").alias("value"))
        .select(
            F.lit("alerts_breakdown").alias("section"),
            F.lit("volcanoes_by_observatory_alert").alias("metric"),
            F.concat_ws("|", F.col("observatory"), F.col("alert_level")).alias("dimension"),
            "value",
            F.lit("volcanoes").alias("unit"),
        )
    )

    by_reg = (
        enriched.groupBy("region", "color_code")
        .agg(F.countDistinct("vnum").cast("double").alias("value"))
        .select(
            F.lit("alerts_breakdown").alias("section"),
            F.lit("volcanoes_by_region_color").alias("metric"),
            F.concat_ws("|", F.col("region"), F.col("color_code")).alias("dimension"),
            "value",
            F.lit("volcanoes").alias("unit"),
        )
    )

    return by_obs.unionByName(by_reg)

def tone_distribution(spark, silver: dict):
    """Breakdown of preprocessed notices by activity tone label (escalating / stable / calming)."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from .silver_reader import latest
    from .tone import lexicon_score

    notices = silver.get("notices")
    if notices is None:
        return None

    lex_udf = F.udf(lexicon_score, T.DoubleType())
    scored = (
        latest(notices)
        .where(F.col("summary_clean").isNotNull())
        .withColumn("score", lex_udf(F.col("summary_clean")))
        .withColumn(
            "label",
            F.when(F.col("score") > 0.15, F.lit("escalating"))
            .when(F.col("score") < -0.15, F.lit("calming"))
            .otherwise(F.lit("stable")),
        )
    )

    counts = scored.groupBy("label").agg(F.count("*").cast("double").alias("records"))
    total_row = counts.agg(F.sum("records").alias("tot")).first()
    total = float(total_row["tot"]) if total_row and total_row["tot"] else 0.0
    if total == 0.0:
        return None

    out_cnt = counts.select(
        F.lit("tone_distribution").alias("section"),
        F.lit("records").alias("metric"),
        F.col("label").alias("dimension"),
        F.col("records").alias("value"),
        F.lit("records").alias("unit"),
    )
    out_pct = counts.select(
        F.lit("tone_distribution").alias("section"),
        F.lit("pct_records").alias("metric"),
        F.col("label").alias("dimension"),
        (F.col("records") * 100.0 / F.lit(total)).alias("value"),
        F.lit("%").alias("unit"),
    )
    return out_cnt.unionByName(out_pct)


def volume_by_source_weekly(spark, silver: dict):
    """Weekly record volume per source (HANS API notices, scraped updates, FDSN earthquakes)."""
    from pyspark.sql import functions as F
    from .silver_reader import latest

    frames = []
    notices = silver.get("notices")
    if notices is not None and "sent_time" in notices.columns:
        frames.append(
            latest(notices)
            .where(F.col("sent_time").isNotNull())
            .groupBy(F.date_trunc("week", "sent_time").alias("period"))
            .agg(F.count("*").cast("double").alias("value"))
            .withColumn("dimension", F.lit("usgs_hans_notices_api"))
        )

    updates = silver.get("volcano_updates")
    if updates is not None:
        ts_col = "issued_time" if "issued_time" in updates.columns else "processed_utc"
        if ts_col in updates.columns:
            frames.append(
                latest(updates)
                .where(F.col(ts_col).isNotNull())
                .groupBy(F.date_trunc("week", ts_col).alias("period"))
                .agg(F.count("*").cast("double").alias("value"))
                .withColumn("dimension", F.lit("usgs_volcano_updates_scraping"))
            )

    eq = silver.get("earthquakes")
    if eq is not None and "event_time" in eq.columns:
        frames.append(
            latest(eq)
            .where(F.col("nearest_vnum").isNotNull() & F.col("event_time").isNotNull())
            .groupBy(F.date_trunc("week", "event_time").alias("period"))
            .agg(F.count("*").cast("double").alias("value"))
            .withColumn("dimension", F.lit("usgs_earthquakes"))
        )

    if not frames:
        return None

    combined = frames[0]
    for f in frames[1:]:
        combined = combined.unionByName(f)

    return combined.select(
        F.lit("volume_activity").alias("section"),
        F.lit("records").alias("metric"),
        "dimension",
        "period",
        "value",
        F.lit("records").alias("unit"),
    )


def narrative_highlights(spark, silver: dict):
    """Key summary figures quoted by the functional-user narrative card."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from .silver_reader import latest
    from .tone import lexicon_score

    rows = []
    notices = silver.get("notices")
    if notices is not None and "sent_time" in notices.columns and "summary_clean" in notices.columns:
        lex_udf = F.udf(lexicon_score, T.DoubleType())
        df = (
            latest(notices)
            .where(F.col("sent_time").isNotNull() & F.col("summary_clean").isNotNull())
            .withColumn("period", F.date_trunc("week", "sent_time"))
            .withColumn("lex", lex_udf(F.col("summary_clean")))
        )
        weeks = [r["period"] for r in df.select("period").distinct().orderBy(F.desc("period")).limit(2).collect()]
        if weeks:
            latest_w = weeks[0]
            curr_mean = df.where(F.col("period") == latest_w).agg(F.avg("lex")).first()[0]
            if curr_mean is not None:
                rows.append(("latest_week_tone", "ALL", latest_w, float(curr_mean), "score"))
            if len(weeks) > 1:
                prev_mean = df.where(F.col("period") == weeks[1]).agg(F.avg("lex")).first()[0]
                if curr_mean is not None and prev_mean is not None:
                    rows.append(("tone_wow_delta", "ALL", latest_w, float(curr_mean - prev_mean), "score"))

            name_col = "volcano_name" if "volcano_name" in df.columns else "vnum"
            top_v = (
                df.where((F.col("period") == latest_w) & F.col(name_col).isNotNull())
                .groupBy(F.col(name_col).cast("string").alias("vlabel"))
                .agg(F.avg("lex").alias("mean_lex"))
                .orderBy(F.desc("mean_lex"))
                .first()
            )
            if top_v and top_v["mean_lex"] is not None:
                rows.append(("top_escalating_volcano", str(top_v["vlabel"]), latest_w, float(top_v["mean_lex"]), "score"))

    alerts = silver.get("alert_notices")
    if alerts is not None and "alert_level_ord" in alerts.columns:
        elev = latest(alerts).where(F.col("alert_level_ord") >= 2).select("vnum").distinct().count()
        rows.append(("elevated_volcanoes_count", "ALL", None, float(elev), "volcanoes"))

    if not rows:
        return None

    schema = "metric string, dimension string, period timestamp, value double, unit string"
    return spark.createDataFrame(rows, schema=schema).select(
        F.lit("narrative_highlights").alias("section"),
        "metric",
        "dimension",
        "period",
        "value",
        "unit",
    )

AGGREGATIONS = [
    earthquakes_weekly,
    tone_weekly,
    top_keywords_bigrams,
    api_vs_scraping_comparison,
    earthquakes_vs_baseline,
    alerts_by_observatory_region,
    tone_distribution,
    volume_by_source_weekly,
    narrative_highlights,
]


def build_storytelling(spark, silver: dict, generated: datetime):
    from .contract import conform, union_all

    from ..config import load_config
    from .indicator import build_indicator

    parts = [agg(spark, silver) for agg in AGGREGATIONS]
    # Workshop 4: the Volcano Activity Indicator sections (vai_current, vai_weekly, volcano_map,
    # eruption_history) travel in the storytelling file, which the storytelling dashboard reads
    parts.append(build_indicator(spark, silver, generated, load_config()["indicator"]))
    return union_all(spark, [conform(p, generated) for p in parts if p is not None])
