WITH job_status AS 
(SELECT J.name
, j.job_id
, s.step_name
, s.step_id
, s.message
, s.run_time
, s.run_status
, s.instance_id
, TRY_CONVERT(DATE, CAST(s.run_date AS VARCHAR(20)),112) run_date
, ROW_NUMBER () OVER (PARTITION BY s.job_id, s.step_id ORDER BY s.run_date DESC, s.run_time DESC) run_instance
FROM msdb.dbo.sysjobs J
JOIN msdb.dbo.sysjobhistory s
ON s.job_id = J.job_id
WHERE J.enabled = 1)
, check_next_instance AS
(SELECT *
FROM job_status s
WHERE s.run_instance = 1
AND s.run_status <> 1
AND NOT EXISTS (SELECT 1
FROM msdb.dbo.sysjobhistory sc
WHERE s.step_id = sc.step_id
AND sc.run_time >= s.run_time
AND sc.job_id = s.job_id
AND sc.run_date >= TRY_CONVERT(INT, TRY_CONVERT(NVARCHAR(10), s.run_date, 112))
AND sc.run_status = 1))
SELECT *
FROM check_next_instance j
ORDER BY CASE WHEN (CHARINDEX('Compass',J.name) > 0
OR CHARINDEX('Oliver',J.name) > 0
OR CHARINDEX('IDM',J.name) > 0
OR CHARINDEX('IDV',J.name) > 0
OR CHARINDEX('Enrolment',J.name) > 0) THEN 1 ELSE 99 END, j.run_date, J.name, j.step_id, run_time


