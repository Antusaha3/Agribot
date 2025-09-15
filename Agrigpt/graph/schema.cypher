// --- Node constraints (IDs must be unique)
CREATE CONSTRAINT crop_id     IF NOT EXISTS FOR (c:Crop)       REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT variety_id  IF NOT EXISTS FOR (v:Variety)    REQUIRE v.id IS UNIQUE;
CREATE CONSTRAINT disease_id  IF NOT EXISTS FOR (d:Disease)    REQUIRE d.id IS UNIQUE;
CREATE CONSTRAINT fert_id     IF NOT EXISTS FOR (f:Fertilizer) REQUIRE f.id IS UNIQUE;
CREATE CONSTRAINT practice_id IF NOT EXISTS FOR (p:Practice)   REQUIRE p.id IS UNIQUE;

// Optional indexes
CREATE INDEX crop_name_en    IF NOT EXISTS FOR (c:Crop)    ON (c.name_en);
CREATE INDEX variety_name_en IF NOT EXISTS FOR (v:Variety) ON (v.name_en);
MATCH (c:Crop)
SET c.slug = coalesce(
  toLower(replace(c.name_en,' ','-')),
  toLower(replace(c.name_bn,' ','-'))
);
CREATE CONSTRAINT crop_id IF NOT EXISTS FOR (c:Crop)       REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT variety_id IF NOT EXISTS FOR (v:Variety) REQUIRE v.id IS UNIQUE;
CREATE CONSTRAINT disease_id IF NOT EXISTS FOR (d:Disease) REQUIRE d.id IS UNIQUE;
CREATE CONSTRAINT fert_id IF NOT EXISTS FOR (f:Fertilizer) REQUIRE f.id IS UNIQUE;
