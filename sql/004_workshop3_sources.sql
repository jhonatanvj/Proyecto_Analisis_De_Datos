-- Workshop 3: sources added to config/sources.yaml (HANS notice texts and the HANS volcano list).
-- Idempotent, applied by the db-migrate service.

INSERT INTO source (source_id, name, url, format, license, citation, role) VALUES
 ('usgs_hans_notices', 'USGS HANS notice search - full text of every volcano notice',
  'https://volcanoes.usgs.gov/hans-public/api/search/search', 'json',
  'Public domain (U.S. Government). Credit: U.S. Geological Survey Volcano Hazards Program',
  'U.S. Geological Survey, Volcano Hazards Program, Hazard Notification System (HANS) public API',
  'incremental'),
 ('usgs_hans_volcanoes', 'USGS HANS public API - US volcano reference list',
  'https://volcanoes.usgs.gov/hans-public/api/volcano/getUSVolcanoes', 'json',
  'Public domain (U.S. Government). Credit: U.S. Geological Survey Volcano Hazards Program',
  'U.S. Geological Survey, Volcano Hazards Program, Hazard Notification System (HANS) public API',
  'optional')
ON CONFLICT (source_id) DO NOTHING;
