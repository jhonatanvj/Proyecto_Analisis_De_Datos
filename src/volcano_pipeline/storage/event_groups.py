"""Event identification: fill event_group with records that describe the same real-world event.

Two rules, parameters in config/sources.yaml (event_matching):

same_quake (rule 'same_quake_cross_network')
    Two earthquake events from *different* networks, at most ``max_seconds`` apart, at most
    ``max_km`` apart and with magnitudes within ``max_mag_diff``, are one earthquake reported
    twice under two ids (e.g. hv61684461 and us10008fuh). Same-network pairs are left alone:
    on the real data they are swarm events seconds apart, not copies. Each event is compared only
    with its next ``neighbours`` events in time (window function), which keeps the run at ~10 s
    for ~300k earthquakes instead of a self-join. group_id = 'quake:<smaller event_id>';
    score = 1 - 0.5 * dt/max_seconds - 0.5 * km/max_km (1 = same time and place).

volcano_episode (rule 'volcano_week')
    For every volcano and ISO week with an alert notice or an eruption start, the group links that
    notice / eruption and the earthquakes within the volcano radius (volcano_earthquake) in the same
    week. group_id = 'vnum:<vnum>:<YYYY-Www>'; score = 1 - distance/radius for earthquakes, 1 for
    the anchors.

Both rules are rebuilt from scratch in one transaction (delete the rule's rows, insert again), so
the result never depends on the load order and changing a parameter only needs a new run.
"""
from __future__ import annotations

import json
import logging

log = logging.getLogger(__name__)

SAME_QUAKE_SQL = """
WITH q AS (
    SELECT e.event_id, e.event_time, e.geom, k.magnitude, k.network
    FROM event e JOIN earthquake k USING (event_id)
    WHERE e.event_type = 'earthquake' AND e.event_time IS NOT NULL),
nb AS (
    SELECT q.event_id, q.event_time, q.geom, q.magnitude, q.network,
           lead(event_id, n) OVER w AS nb_id, lead(event_time, n) OVER w AS nb_time,
           lead(geom, n) OVER w AS nb_geom, lead(magnitude, n) OVER w AS nb_mag, lead(network, n) OVER w AS nb_net
    FROM q CROSS JOIN generate_series(1, %(neighbours)s) AS n
    WINDOW w AS (PARTITION BY n ORDER BY event_time, event_id)),
pairs AS (
    SELECT least(event_id, nb_id) AS a, greatest(event_id, nb_id) AS b,
           extract(epoch FROM nb_time - event_time) AS dt, ST_Distance(geom, nb_geom) / 1000 AS km
    FROM nb
    WHERE nb_id IS NOT NULL AND network IS DISTINCT FROM nb_net
      AND nb_time - event_time <= make_interval(secs => %(max_seconds)s)
      AND ST_DWithin(geom, nb_geom, %(max_km)s * 1000)
      AND abs(magnitude - nb_mag) <= %(max_mag_diff)s),
scored AS (
    SELECT a, b, 1 - 0.5 * dt / %(max_seconds)s - 0.5 * km / %(max_km)s AS score FROM pairs)
INSERT INTO event_group (group_id, event_id, rule, score)
SELECT 'quake:' || a, member, 'same_quake_cross_network', round(max(score)::numeric, 3)
FROM scored CROSS JOIN LATERAL (VALUES (a), (b)) AS m(member)
GROUP BY a, member          -- an event can pair with two later ones (A-B, A-C): one row per member
ON CONFLICT (group_id, event_id) DO NOTHING
"""

VOLCANO_EPISODE_SQL = """
WITH anchors AS (
    SELECT a.volcano_number AS vnum, e.event_id, e.event_time FROM volcano_alert a JOIN event e USING (event_id)
    WHERE 'alert_notice' = ANY(%(anchor_types)s)
    UNION ALL
    SELECT r.volcano_number, e.event_id, e.event_time FROM eruption r JOIN event e USING (event_id)
    WHERE 'eruption' = ANY(%(anchor_types)s) AND e.event_time IS NOT NULL),
weeks AS (
    SELECT DISTINCT vnum, date_trunc('week', event_time) AS week FROM anchors),
members AS (
    SELECT a.vnum, date_trunc('week', a.event_time) AS week, a.event_id, 1.0::float8 AS score FROM anchors a
    UNION ALL
    SELECT w.vnum, w.week, ve.event_id, greatest(0, 1 - ve.distance_km / %(radius_km)s)
    FROM weeks w
    JOIN volcano_earthquake ve ON ve.volcano_number = w.vnum
    JOIN event e ON e.event_id = ve.event_id
    WHERE e.event_time >= w.week AND e.event_time < w.week + interval '7 days')
INSERT INTO event_group (group_id, event_id, rule, score)
SELECT 'vnum:' || vnum || ':' || to_char(week, 'IYYY-"W"IW'), event_id, 'volcano_week', round(score::numeric, 3)
FROM members
ON CONFLICT (group_id, event_id) DO NOTHING
"""


def rebuild_event_groups(conn, cfg: dict) -> dict:
    """Recompute both rules in one transaction. Returns rows and groups per rule."""
    rules = cfg.get("event_matching") or {}
    quake = {"neighbours": 3, "max_seconds": 16, "max_km": 10, "max_mag_diff": 0.5} | (rules.get("same_quake") or {})
    episode = {"anchor_types": ["alert_notice", "eruption"]} | (rules.get("volcano_episode") or {})
    episode["radius_km"] = float(cfg.get("spatial", {}).get("volcano_radius_km", 30))
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("DELETE FROM event_group WHERE rule IN ('same_quake_cross_network', 'volcano_week')")
        cur.execute(SAME_QUAKE_SQL, quake)
        cur.execute(VOLCANO_EPISODE_SQL, episode)
        cur.execute("SELECT rule, count(*), count(DISTINCT group_id) FROM event_group GROUP BY rule")
        summary = {rule: {"rows": rows, "groups": groups} for rule, rows, groups in cur.fetchall()}
    log.info(json.dumps({"event": "event_groups", **summary}))
    return summary
