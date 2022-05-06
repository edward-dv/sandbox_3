WITH prep AS (
SELECT 
  DISTINCT user_id
  , LAST_VALUE(engagement_status) OVER (PARTITION BY user_id ORDER BY transaction_week
   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS current_status
  , MAX(activation_week_index) OVER (PARTITION BY user_id)                              AS weeks_since_activation
FROM `analytics-take-home-test.monzo_product_edv.user_engagement_status`
)
SELECT 
  DISTINCT prep.user_id
  , CASE 
      WHEN current_status = 'Churned'
      THEN 1
      ELSE 0
  END AS churn_flag
  , weeks_since_activation
  , CASE 
      WHEN friends_on_monzo > 0
      THEN 1
      ELSE 0
  END AS friends_flag
  , CASE 
      WHEN age < 30
      THEN 1
      ELSE 0
  END AS young_flag
  , CASE 
      WHEN profile_photo_upload IS NOT NULL
      THEN 1
      ELSE 0
  END AS photo_flag
  , CASE 
      WHEN android_pay_activated IS NOT NULL
      THEN 1
      ELSE 0
  END AS android_pay_flag
  , CASE
      WHEN offered_overdraft > 0
      THEN 1
      ELSE 0
  END AS overdraft_flag
  , CASE 
      WHEN SUM(chat_conversations) OVER (PARTITION BY prep.user_id) > 2
      THEN 1
      ELSE 0
  END AS chats_flag
FROM prep
JOIN `analytics-take-home-test.monzo_product.users`                       AS u
  ON prep.user_id = u.user_id
JOIN `analytics-take-home-test.monzo_product.user_activity_daily`         AS uad
  ON prep.user_id = uad.user_id
ORDER BY 1