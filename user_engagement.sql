-- ENGAGEMENT STATUS FLAG --
WITH
-- Generate date array
dates AS (
  SELECT 
    user_id
    , week 
  FROM `analytics-take-home-test.monzo_product.transactions`
  -- Hard coding dates as temporary solution.
  LEFT JOIN UNNEST((SELECT 
          GENERATE_DATE_ARRAY('2017-10-29', '2018-06-09', INTERVAL 1 WEEK)
        )) AS week
  GROUP BY 1,2
)
-- Count transactions and add date array
, prep AS (
  SELECT 
    user_id
    , DATE_TRUNC(CAST(timestamp AS DATE), WEEK) AS transaction_week
    , COUNT(amount)                             AS count_transactions
  FROM `analytics-take-home-test.monzo_product.transactions` 
  WHERE timestamp BETWEEN '2017-10-29' AND '2018-06-09'
  GROUP BY 1,2

  UNION ALL

  SELECT
    user_id
    , week AS transaction_week
    , 0 AS count_transactions
  FROM dates
)
-- Aggregate
, prep_2 AS (
  SELECT 
    user_id
    , transaction_week
    , MAX(count_transactions) AS count_transactions
  FROM prep
  GROUP BY 1,2
)
-- Create two cohorts based on activation and first transaction
, cohorts_1 AS (
  SELECT 
    p2.user_id
    , MIN(IF(count_transactions > 0, transaction_week, null)) OVER (PARTITION BY p2.user_id)                                      AS transaction_cohort_week
    , DATE_TRUNC(CAST(account_activation AS DATE), WEEK)                                                                          AS activation_cohort_week
    , transaction_week
    , DATE_DIFF(transaction_week, MIN(IF(count_transactions > 0, transaction_week, null)) OVER (PARTITION BY p2.user_id), WEEK)   AS transaction_week_index
    , DATE_DIFF(transaction_week, DATE_TRUNC(CAST(account_activation AS DATE), WEEK), WEEK)                                       AS activation_week_index
    , count_transactions
  FROM prep_2 AS p2
  JOIN `analytics-take-home-test.monzo_product.users` AS u
    ON p2.user_id = u.user_id
  WHERE account_activation >= '2017-10-29'
)
-- Create conditions for engagement status flag
, conditions AS (
  SELECT 
    c.*
    , CASE 
        WHEN AVG(count_transactions) OVER (PARTITION BY user_id ORDER BY transaction_week ROWS 3 PRECEDING) = 0
        AND activation_week_index >= 3 
        THEN TRUE
        ELSE FALSE
      END         AS no_trx_4_weeks
  FROM cohorts_1  AS c
  WHERE transaction_week >= activation_cohort_week
)
-- Create engagement status flag
SELECT
  user_id
  , transaction_cohort_week
  ,  activation_cohort_week
  , transaction_week
  , transaction_week_index
  , activation_week_index
  , count_transactions
  , CASE
      WHEN count_transactions >= 7 THEN 'Very Active'
      WHEN count_transactions BETWEEN 1 AND 7 THEN 'Active'
      WHEN no_trx_4_weeks = TRUE THEN 'Churned'
      WHEN count_transactions = 0 THEN 'Inactive'
    END AS engagement_status
FROM conditions
ORDER BY 1,3
