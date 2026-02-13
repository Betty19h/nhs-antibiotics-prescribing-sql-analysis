-- 1) National trend over time (items + cost)
SELECT
  month,
  SUM(total_items) AS items,
  ROUND(SUM(total_cost)::numeric, 2) AS cost
FROM mart.icb_monthly
GROUP BY 1
ORDER BY 1;

-- 2) Top 15 ICBs by total items (latest month)
WITH latest AS (SELECT MAX(month) AS month FROM mart.icb_monthly)
SELECT
  m.icb_name,
  m.total_items,
  ROUND(m.total_cost::numeric, 2) AS total_cost,
  m.cost_per_item
FROM mart.icb_monthly m
JOIN latest l ON m.month = l.month
ORDER BY m.total_items DESC
LIMIT 15;

-- 3) Biggest month-to-month changes (absolute + % change)
WITH t AS (
  SELECT
    icb_code,
    icb_name,
    month,
    total_items,
    LAG(total_items) OVER (PARTITION BY icb_code ORDER BY month) AS prev_items
  FROM mart.icb_monthly
)
SELECT
  icb_name,
  month,
  total_items,
  prev_items,
  (total_items - prev_items) AS change_items,
  CASE WHEN prev_items > 0
    THEN ROUND((total_items - prev_items)::numeric / prev_items * 100, 2)
  END AS pct_change
FROM t
WHERE prev_items IS NOT NULL
ORDER BY ABS(total_items - prev_items) DESC
LIMIT 25;

-- 4) Outlier ICBs for total items (z-score) in latest month
WITH latest AS (SELECT MAX(month) AS month FROM mart.icb_monthly),
stats AS (
  SELECT
    AVG(total_items) AS mean_items,
    STDDEV_SAMP(total_items) AS sd_items
  FROM mart.icb_monthly m
  JOIN latest l ON m.month = l.month
),
latest_rows AS (
  SELECT m.*
  FROM mart.icb_monthly m
  JOIN latest l ON m.month = l.month
)
SELECT
  icb_name,
  total_items,
  ROUND((total_items - s.mean_items) / NULLIF(s.sd_items, 0), 2) AS z_score
FROM latest_rows r
CROSS JOIN stats s
WHERE s.sd_items IS NOT NULL
ORDER BY z_score DESC
LIMIT 20;

-- 5) Highest cost per item (latest month) with a minimum volume filter
WITH latest AS (SELECT MAX(month) AS month FROM mart.icb_monthly)
SELECT
  icb_name,
  total_items,
  cost_per_item
FROM mart.icb_monthly m
JOIN latest l ON m.month = l.month
WHERE total_items >= 5000
ORDER BY cost_per_item DESC
LIMIT 20;
