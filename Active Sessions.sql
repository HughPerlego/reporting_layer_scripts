/* Active Sessions Days */


SELECT user_id, COUNT(DISTINCT(session_id)) AS session_count
FROM reporting.user_tracking_events
WHERE user_id in (SELECT user_id
                 FROM reporting.subscriptions)
GROUP BY user_id

