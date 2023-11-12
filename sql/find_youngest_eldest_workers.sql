-- 1 спосіб
SELECT 'OLDEST' AS "TYPE", "name", birthday 
FROM worker w 
WHERE birthday = (
	SELECT min(birthday)
	FROM worker
)
UNION 
SELECT 'YOUNGEST' AS "TYPE", "name", birthday 
FROM worker w 
WHERE birthday = (
	SELECT max(birthday)
	FROM worker
)
ORDER BY birthday ASC, "name" ASC;



-- 2 спосіб
-- SELECT 
-- 	CASE 
-- 		WHEN birthday = (
-- 			SELECT min(birthday)
-- 			FROM worker
-- 		) THEN 'OLDEST'
-- 		WHEN birthday = (
-- 			SELECT max(birthday)
-- 			FROM worker
-- 		) THEN 'YOUNGEST'
-- 	END AS "TYPE", "name", birthday
-- FROM worker
-- WHERE 
-- birthday = 
-- (
-- 	SELECT max(birthday)
-- 	FROM worker
-- ) OR 
-- birthday =
-- (
-- 	SELECT min(birthday)
-- 	FROM worker
-- )
-- ORDER BY birthday ASC, "name" ASC;