/* To be used as main data table in reporting layer - for reporting how many days / weeks from the subscription
   creation event a user has until they become churned */

/* Create Base Data Table - Limit to B2C users and those which only appear twice -
so no reactivated subscribers
*/

insert into reporting_layer.subscriber_b2c_churned_date (WITH Main_Data_Table AS (
SELECT user_id, 
        MIN(start_timestamp) AS First_subscription_start_date,
        MIN(end_timestamp) AS First_subscription_end_date,
        MIN(subscription_canceled_at) AS First_subscription_cancelled_date,
        MIN(paid_start_timestamp) AS First_paid_start_date,
        MIN(paid_end_timestamp) AS First_paid_end_date,
        SUM(total_paid_gbp) AS Subscription_total_paid,
        SUM(paid_months) AS Subscription_Paid_months
FROM reporting.subscriptions 
WHERE payment_channel = 'direct'
AND user_id IN (SELECT user_id 
                FROM reporting.subscriptions 
                GROUP BY user_id 
                HAVING COUNT(*)  <= 2)
GROUP BY user_id
),

/* Mark subscription churned date - 

- first preferece will be the first cancelled_at date as the user has activly chosen to cancel 
- if user became paying subsciber than paid end date will be used as this will be the last date the user willingly paid for 
- if user did not become paying subscriber then first subscription end date is used - which should be the end date of the trial */

                                                         Subscription_Churned_Date_Table AS (
SELECT Main_Data_Table.*, CASE WHEN First_subscription_cancelled_date IS NOT NULL THEN First_subscription_cancelled_date
               WHEN First_paid_end_date IS NOT NULL THEN First_paid_end_date
               ELSE First_subscription_end_date END AS Subscriber_Churned_date
FROM Main_Data_Table
),

/* end dates above current date means subscriber is still active - so weeks till churned marked as 99999 */

                                                         Days_Till_Churned_Table AS (
SELECT *, 
CASE WHEN Subscriber_Churned_date >= GETDATE() THEN 99999 ELSE DATEDIFF(days, First_subscription_start_date, Subscriber_Churned_date) END AS Days_Till_Churned,
CASE WHEN Subscriber_Churned_date >= GETDATE() THEN 99999 ELSE DATEDIFF(weeks, First_subscription_start_date, Subscriber_Churned_date) END AS Weeks_Till_Churned,
CASE WHEN Subscriber_Churned_date >= GETDATE() THEN 'Y' ELSE 'N' END AS Subscriber_Active
FROM Subscription_Churned_Date_Table)

SELECT *
FROM Days_Till_Churned_Table)
