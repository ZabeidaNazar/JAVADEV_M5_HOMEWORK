-- 1 спосіб
SELECT "name", EXTRACT (YEAR FROM age(finish_date, start_date)) * 12 + EXTRACT (MONTH FROM age(finish_date, start_date)) AS month_count
FROM project p 
WHERE EXTRACT (YEAR FROM age(finish_date, start_date)) * 12 + EXTRACT (MONTH FROM age(finish_date, start_date)) = (
	SELECT max(EXTRACT (YEAR FROM age(finish_date, start_date)) * 12 + EXTRACT (MONTH FROM age(finish_date, start_date))) 
	FROM project
)
ORDER BY id ASC;



-- 2 спосіб - працює у H2, але не працює у PostgreSQL
-- SELECT "name", DATEDIFF(finish_date, start_date, MONTH) AS month_count
-- FROM project p 
-- WHERE DATEDIFF(finish_date, start_date, MONTH) = (
-- 	SELECT max(DATEDIFF(finish_date, start_date, MONTH)) 
-- 	FROM project
-- )
-- ORDER BY id ASC;

