SELECT
	p."name", (EXTRACT (YEAR FROM age(finish_date, start_date)) * 12 + EXTRACT (MONTH FROM age(finish_date, start_date))) *
	sum(w.salary) AS price
FROM project p
JOIN project_worker pw ON p.id = pw.project_id
JOIN worker w ON pw.worker_id = w.id
GROUP BY p.id
ORDER BY price DESC;