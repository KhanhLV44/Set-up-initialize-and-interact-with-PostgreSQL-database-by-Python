-----------------------------------------------------------------------------------------------------------
-- 0. DELETE TABLES
DROP TABLE agg_order_count_by_status,
	agg_daily_revenue,
	agg_daily_product_revenue,
	ex_student_scores;

-----------------------------------------------------------------------------------------------------------
-- 1. CREATE AGGREGATED TABLES
-- table "agg_order_count_by_status"
CREATE TABLE agg_order_count_by_status
AS
	SELECT order_status, COUNT(*) AS order_count FROM orders
	GROUP BY 1;

-- table "agg_daily_revenue"
CREATE TABLE agg_daily_revenue
AS
	SELECT o.order_date, ROUND(SUM(oi.order_item_subtotal)::NUMERIC, 2) AS order_revenue
	FROM orders AS o
	JOIN order_items AS oi ON o.order_id = oi.order_item_order_id
	WHERE o.order_status IN ('COMPLETE', 'CLOSED')
	GROUP BY 1;

-- table "agg_daily_product_revenue"
CREATE TABLE agg_daily_product_revenue
AS
	SELECT o.order_date,
			oi.order_item_product_id,
			ROUND(SUM(oi.order_item_subtotal)::NUMERIC, 2) AS order_revenue
	FROM orders AS o
	JOIN order_items AS oi ON o.order_id = oi.order_item_order_id
	WHERE o.order_status IN ('COMPLETE', 'CLOSED')
	GROUP BY 1, 2;

-----------------------------------------------------------------------------------------------------------
-- 2. AGGREGATION USING 'OVER' AND 'PARTITION BY'
-- query 1
SELECT dr.*, SUM(order_revenue) OVER (PARTITION BY 1) AS total_order_revenue
FROM agg_daily_revenue AS dr
ORDER BY 1;

-- query 2
SELECT order_date,
       order_item_product_id,
       order_revenue,
       RANK() OVER (ORDER BY order_revenue DESC) AS rnk,
       DENSE_RANK() OVER (ORDER BY order_revenue DESC) AS drnk
FROM agg_daily_product_revenue
WHERE to_char(order_date::date, 'yyyy-MM') = '2014-01'
ORDER BY order_revenue DESC;

-- query 3
WITH agg_daily_product_revenue_ranks AS (
	SELECT order_date,
	       order_item_product_id,
	       order_revenue,
	       RANK() OVER (ORDER BY order_revenue DESC) AS rnk,
	       DENSE_RANK() OVER (ORDER BY order_revenue DESC) AS drnk
	FROM agg_daily_product_revenue
	WHERE order_date = '2014-01-01 00:00:00.0'
	ORDER BY order_revenue DESC
)
SELECT * FROM agg_daily_product_revenue_ranks
WHERE drnk <= 5
ORDER BY order_revenue DESC;

-- table "ex_student_scores"
CREATE TABLE ex_student_scores (
	student_id INT PRIMARY KEY,
	student_score INT
);

INSERT INTO ex_student_scores VALUES (1, 980), (2, 960), (3, 960), (4, 990), (5, 920),
	                                 (6, 960), (7, 980), (8, 960), (9, 940), (10, 940);
-- THE END!