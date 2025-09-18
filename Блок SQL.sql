WITH monthly_data AS (SELECT 
		DATE_FORMAT(t.date_new, '%Y-%m') AS month,
        t.id_check,
        t.id_client,
        t.sum_payment,
        c.gender
FROM transactions t
JOIN customerinfo_0002 c ON t.id_client = c.id_client),

aggregated AS (SELECT
        month,
        COUNT(id_check) AS total_operations,
        COUNT(DISTINCT id_client) AS active_clients,
        SUM(sum_payment) AS total_sum,
        AVG(sum_payment) AS avg_check
FROM monthly_data
GROUP BY month),

year_totals AS (SELECT
        COUNT(id_check) AS year_total_operations,
        SUM(sum_payment) AS year_total_sum
FROM monthly_data),

gender_monthly AS (SELECT
        month,
        gender,
        COUNT(DISTINCT id_client) AS client_count,
        SUM(sum_payment) AS gender_sum
FROM monthly_data
GROUP BY month, gender),

gender_totals AS (SELECT
        month,
        SUM(client_count) OVER (PARTITION BY month) AS total_clients_per_month,
        SUM(gender_sum) OVER (PARTITION BY month) AS total_sum_per_month
FROM gender_monthly)
#2
SELECT 
    a.month,
    a.avg_check,
    a.total_operations,
    a.active_clients,
    ROUND(a.total_operations / y.year_total_operations * 100, 2) AS operations_share_percent,
    ROUND(a.total_sum / y.year_total_sum * 100, 2) AS sum_share_percent,
    gm.gender,
    ROUND(gm.client_count / gt.total_clients_per_month * 100, 2) AS gender_share_percent,
    ROUND(gm.gender_sum / gt.total_sum_per_month * 100, 2) AS gender_spending_share_percent
FROM aggregated a
JOIN year_totals y ON 1=1
JOIN gender_monthly gm ON a.month = gm.month
JOIN gender_totals gt ON gm.month = gt.month
ORDER BY a.month, gm.gender;

#3
-- Общая таблица с нужными полями
WITH enriched_data AS (
    SELECT 
        t.id_check,
        t.sum_payment,
        t.date_new,
        c.age,
        CASE
            WHEN c.age IS NULL THEN 'Unknown'
            WHEN c.age < 10 THEN '0-9'
            WHEN c.age BETWEEN 10 AND 19 THEN '10-19'
            WHEN c.age BETWEEN 20 AND 29 THEN '20-29'
            WHEN c.age BETWEEN 30 AND 39 THEN '30-39'
            WHEN c.age BETWEEN 40 AND 49 THEN '40-49'
            WHEN c.age BETWEEN 50 AND 59 THEN '50-59'
            WHEN c.age BETWEEN 60 AND 69 THEN '60-69'
            WHEN c.age >= 70 THEN '70+'
        END AS age_group,
        CONCAT(YEAR(t.date_new), '-Q', QUARTER(t.date_new)) AS quarter
    FROM 
        transactions t
    JOIN 
        customerinfo_0002 c ON t.id_client = c.id_client
    WHERE 
        t.date_new >= '2015-06-01' AND t.date_new < '2016-06-01'
),

-- Общие суммы и кол-во операций по возрастным группам
total_by_age AS (
    SELECT 
        age_group,
        COUNT(id_check) AS total_operations,
        SUM(sum_payment) AS total_sum
    FROM 
        enriched_data
    GROUP BY 
        age_group
),

-- Общие суммы и операции по кварталам и возрастным группам
quarterly_by_age AS (
    SELECT 
        age_group,
        quarter,
        COUNT(id_check) AS quarter_operations,
        SUM(sum_payment) AS quarter_sum,
        AVG(sum_payment) AS avg_check
    FROM 
        enriched_data
    GROUP BY 
        age_group, quarter
),

-- Общие суммы по кварталам (всего)
quarter_totals AS (
    SELECT 
        quarter,
        COUNT(id_check) AS total_ops_q,
        SUM(sum_payment) AS total_sum_q
    FROM 
        enriched_data
    GROUP BY 
        quarter
)

-- Финальный запрос
SELECT 
    q.age_group,
    q.quarter,
    q.quarter_operations,
    q.quarter_sum,
    ROUND(q.avg_check, 2) AS avg_check,
    ROUND(q.quarter_operations / qt.total_ops_q * 100, 2) AS op_share_percent,
    ROUND(q.quarter_sum / qt.total_sum_q * 100, 2) AS sum_share_percent
FROM 
    quarterly_by_age q
JOIN 
    quarter_totals qt ON q.quarter = qt.quarter
ORDER BY 
    q.age_group, q.quarter;
