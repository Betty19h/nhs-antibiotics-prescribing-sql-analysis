\copy (SELECT month, SUM(total_items) AS items, ROUND(SUM(total_cost)::numeric,2) AS cost FROM mart.icb_monthly GROUP BY 1 ORDER BY 1) TO 'outputs/national_trend.csv' WITH (FORMAT csv, HEADER true);

\copy (WITH latest AS (SELECT MAX(month) AS month FROM mart.icb_monthly) SELECT icb_name, total_items, ROUND(total_cost::numeric,2) AS total_cost, cost_per_item FROM mart.icb_monthly m JOIN latest l ON m.month = l.month ORDER BY total_items DESC LIMIT 15) TO 'outputs/top_icbs_latest_month.csv' WITH (FORMAT csv, HEADER true);

\copy (WITH latest AS (SELECT MAX(month) AS month FROM mart.icb_monthly), stats AS (SELECT AVG(total_items) AS mean_items, STDDEV_SAMP(total_items) AS sd_items FROM mart.icb_monthly m JOIN latest l ON m.month = l.month), latest_rows AS (SELECT m.* FROM mart.icb_monthly m JOIN latest l ON m.month = l.month) SELECT icb_name, total_items, ROUND((total_items - s.mean_items) / NULLIF(s.sd_items, 0), 2) AS z_score FROM latest_rows r CROSS JOIN stats s WHERE s.sd_items IS NOT NULL ORDER BY z_score DESC LIMIT 20) TO 'outputs/outlier_icbs_latest_month.csv' WITH (FORMAT csv, HEADER true);

