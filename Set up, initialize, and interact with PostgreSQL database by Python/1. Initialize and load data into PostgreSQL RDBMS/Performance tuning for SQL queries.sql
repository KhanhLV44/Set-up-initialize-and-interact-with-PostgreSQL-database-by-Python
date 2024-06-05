----------------------------------------------------------------------------------------------------------------
-- EXPLAIN and EXPLAIN ANALYZE for SQL queries
EXPLAIN
	SELECT * FROM orders WHERE order_id = 2;

EXPLAIN
	SELECT o.*, ROUND(SUM(oi.order_item_subtotal)::numeric, 2) AS revenue
	FROM orders AS o
	JOIN order_items AS oi ON o.order_id = oi.order_item_order_id
	WHERE o.order_id = 2
	GROUP BY o.order_id, o.order_date, o.order_customer_id, o.order_status;

----------------------------------------------------------------------------------------------------------------
-- Using INDEXES to promote performance
DROP INDEX orders_order_customer_id_idx;
DROP INDEX order_items_order_item_order_id_idx;
COMMIT;
	/*
	COMMIT, to save the changes.
	ROLLBACK, to roll back the changes.
		      This command can only undo changes since the last COMMIT or ROLLBACK.
	SAVEPOINT, creates savepoints in which to ROLLBACK.
			   Usually, when execute the ROLLBACK command, it undoes the changes until the last COMMIT or ROLLBACK.
			   But, if you create save points you can partially roll the change back to these points,
			   and you can create multiple save points between two commits.
			   For example, SAVEPOINT savepoint_name; ROLLBACK TO savepoint_name;
	*/

-- Performance testing using stored procedure
CREATE OR REPLACE PROCEDURE perfdemo()
LANGUAGE plpgsql
AS $$
DECLARE
	cur_order_details CURSOR (a_customer_id INT) FOR
		SELECT COUNT(*)
		FROM orders AS o
			JOIN order_items AS oi ON o.order_id = oi.order_item_order_id
		WHERE o.order_customer_id = a_customer_id;
	v_customer_id INT := 0;
	v_order_item_count INT;
BEGIN
	LOOP
		EXIT WHEN v_customer_id >= 1000;
		OPEN cur_order_details(v_customer_id);
		FETCH cur_order_details INTO v_order_item_count;
		v_customer_id = v_customer_id + 1;
		CLOSE cur_order_details;
	END LOOP;
END;
$$;

DO $$
BEGIN
	CALL perfdemo();
END;
$$;

-- Add required indexes to tune performance
CREATE INDEX orders_order_customer_id_idx
ON orders (order_customer_id)

CREATE INDEX order_items_order_item_order_id_idx
ON order_items (order_item_order_id)

ROLLBACK;
-- THE END!