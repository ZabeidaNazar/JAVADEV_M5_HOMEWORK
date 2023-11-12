-- 1 спосіб
SELECT c."name", count(p.id) AS projects_count
FROM client c
JOIN project p ON p.client_id = c.id
GROUP BY c.id
HAVING count(p.id) = (
	SELECT max(projects_count)
	FROM (
		SELECT count(id) AS projects_count 
		FROM project
		GROUP BY client_id
	)
)
ORDER BY "name" ASC;



-- 2 спосіб
-- SELECT "name", projects_count 
-- FROM client c
-- JOIN (
-- 	SELECT client_id, count(p.id) AS projects_count 
-- 	FROM project p
-- 	GROUP BY client_id
-- ) AS count_table ON count_table.client_id = c.id
-- WHERE projects_count = (
-- 	SELECT max(projects_count)
-- 	FROM (
-- 		SELECT count(id) AS projects_count 
-- 		FROM project
-- 		GROUP BY client_id
-- 	)
-- )
-- ORDER BY "name" ASC;



-- 3 спосіб
-- SELECT "name", projects_count
-- FROM (
-- 	SELECT client_id, count(id) AS projects_count 
-- 	FROM project
-- 	GROUP BY client_id
-- 	ORDER BY count(id) DESC
-- )
-- JOIN client c ON client_id = c.id
-- WHERE projects_count = (
-- 	SELECT max(projects_count)
-- 	FROM (
-- 		SELECT count(id) AS projects_count 
-- 		FROM project
-- 		GROUP BY client_id
-- 	)
-- )
-- ORDER BY "name" ASC;