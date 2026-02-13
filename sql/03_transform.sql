DROP TABLE IF EXISTS staging.epd_clean;

CREATE TABLE staging.epd_clean AS
SELECT
  to_date(year_month || '-01', 'YYYY-MM-DD') AS month,
  icb_name,
  icb_code,
  practice_code,
  practice_name,
  postcode,
  bnf_chemical_substance,
  bnf_presentation_name,
  bnf_chapter_plus_code,
  NULLIF(quantity,'')::numeric(18,3) AS quantity,
  NULLIF(items,'')::int AS items,
  NULLIF(nic,'')::numeric(12,2) AS nic,
  NULLIF(actual_cost,'')::numeric(12,2) AS actual_cost,
  snomed_code
FROM raw.epd_antibiotics;

CREATE INDEX IF NOT EXISTS ix_clean_month ON staging.epd_clean(month);
CREATE INDEX IF NOT EXISTS ix_clean_icb ON staging.epd_clean(icb_code);
CREATE INDEX IF NOT EXISTS ix_clean_practice ON staging.epd_clean(practice_code);

-- main analysis table: monthly totals by ICB
DROP TABLE IF EXISTS mart.icb_monthly;

CREATE TABLE mart.icb_monthly AS
SELECT
  month,
  icb_code,
  icb_name,
  SUM(items) AS total_items,
  SUM(actual_cost) AS total_cost,
  CASE WHEN SUM(items) > 0 THEN ROUND(SUM(actual_cost) / SUM(items), 4) END AS cost_per_item
FROM staging.epd_clean
GROUP BY 1,2,3;

CREATE INDEX IF NOT EXISTS ix_icb_month ON mart.icb_monthly(month);
CREATE INDEX IF NOT EXISTS ix_icb_code ON mart.icb_monthly(icb_code);
