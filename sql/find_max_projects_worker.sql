SELECT "name", projects_count
FROM (
	SELECT "name", count(pw.project_id) AS projects_count FROM worker w
	JOIN project_worker pw ON w.id = pw.worker_id 
	GROUP BY id, name
	ORDER BY count(pw.project_id) DESC
)
WHERE projects_count = (
	SELECT max(projects_count) FROM (
		SELECT count(project_id) AS projects_count FROM project_worker
		GROUP BY worker_id
	)
)
ORDER BY "name" ASC;