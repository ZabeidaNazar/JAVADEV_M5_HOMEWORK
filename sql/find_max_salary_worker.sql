SELECT "name", salary FROM worker
WHERE salary = (
	SELECT max(salary) FROM worker
);