CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS raw.epd_antibiotics;

CREATE TABLE raw.epd_antibiotics (
  year_month text,
  regional_office_name text,
  regional_office_code text,
  icb_name text,
  icb_code text,
  pco_name text,
  pco_code text,
  practice_name text,
  practice_code text,
  postcode text,
  bnf_chemical_substance text,
  bnf_presentation_name text,
  bnf_chapter_plus_code text,
  quantity text,
  items text,
  nic text,
  actual_cost text,
  snomed_code text
);

