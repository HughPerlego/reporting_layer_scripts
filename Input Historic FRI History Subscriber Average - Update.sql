INSERT INTO "perlego-analytics-prod".reporting_layer.user_rfi_history_subscriber_average (WITH Subscribe_Change AS (
        SELECT reporting.subscriptions.*,
               CASE
                   WHEN payment_channel = 'direct' AND paid_start_timestamp IS NOT NULL THEN paid_start_timestamp
                   ELSE start_timestamp END AS Combined_Start_Date,
               CASE
                   WHEN payment_channel = 'direct' AND paid_end_timestamp IS NOT NULL THEN paid_end_timestamp
                   ELSE end_timestamp END   AS Combined_End_Date,
               CAST('2022-02-28' as date)   AS Cut_Off_Date
        FROM reporting.subscriptions
        WHERE reporting.subscriptions.subscription_type = 'subscribed'
          AND start_timestamp >= '2017-01-01'
          AND subscription_canceled_at IS NULL),

    /* If end date is past cut off date then use cut off date as reference and add the number of months difference
    from the start and cut off date - to get last reporting date that can be used for that user */

                                                                       Subscribers_Reporting AS (
             SELECT Subscribe_Change.*,

                    CASE
                        WHEN Combined_End_Date >= Cut_Off_Date
                            OR (date_part(MONTH, Combined_End_Date) = date_part(MONTH, GETDATE())
                                AND date_part(YEAR, Combined_End_Date) = date_part(YEAR, GETDATE()))
                            THEN dateadd(month, DATEDIFF(month, combined_start_date, Cut_Off_Date), combined_start_date)

                        ELSE Combined_End_Date
                        END AS Reporting_End_Date
             FROM Subscribe_Change),

/* Work out how many reporting months fit within the range - to use to estimate average per month */
                                                                       Reporting_Month_Cal AS (
             SELECT Subscribers_Reporting.*,
                    DATEDIFF(MONTH, combined_start_date, reporting_end_date) AS Reporting_Months
             FROM Subscribers_Reporting),

/* Filter to subscribers who have been subscribed for at least one full month -
also filter to active subscriptions only - where reporting cut off date is within one month of the overall cut off date
(done to remove some edge cases of users who have subscribed and un subscribed more than once)*/
                                                                       Subscribers AS (
             SELECT *
             FROM Reporting_Month_Cal
             WHERE Reporting_Months > 0
               AND reporting_end_date >= dateadd(month, -1, Cut_Off_Date)),

    /* Last login calculation - work out last session times by user id */
                                                                       Recorded_Tracking_Dates AS (
      select user_id, max(cast(event_timestamp as date)) as last_active_date
      from reporting.user_tracking_events
      where event_name not in ('payment failed', 'payment recieved', 'payment refunded', 'page close')
      and user_id in (select user_id from Subscribers where event_timestamp <= Reporting_End_Date)
      group by user_id),

/* Frequency Calculation
 Count active reading days per user & seconds read - within reporting period
   Only include those with duration above 0 - so had to stay on the page for five seconds or longer*/
                                                                       Reading_Activity AS (
             SELECT reporting.reading_activity_daily.user_id,
                    Reporting_Months,
                    1.0 * COUNT(DISTINCT (cast(reading_date AS DATE)))                  AS Active_Reading_Days,
                    1.0 * ((SUM(reading_activity_daily.reading_duration_seconds))) / 60 AS Total_Mins_Read
             FROM reporting.reading_activity_daily
                      INNER JOIN Subscribers ON Subscribers.user_id = reporting.reading_activity_daily.user_id
             WHERE reading_activity_daily.reading_date >= Subscribers.Combined_Start_Date
               AND reading_activity_daily.reading_date <= Subscribers.Reporting_End_Date
               AND reading_activity_daily.reading_duration_seconds > 0
             GROUP BY reporting.reading_activity_daily.user_id, Reporting_Months),
                                                                       Reading_Activity_Average AS (
             SELECT user_id,
                    Reporting_Months,
                    Active_Reading_Days                                      AS Total_Reading_Days,
                    Total_Mins_Read                                          AS Total_Mins_Read,
                    ROUND(1.0 * (Active_Reading_Days / Reporting_Months), 1) AS AVG_Month_Reading_Days,
                    ROUND(1.0 * (Total_Mins_Read / Reporting_Months), 1)     AS AVG_Month_Mins_Read
             FROM Reading_Activity),

/* Join Tables Together - mark combined last active time as last active time in user summary table - if null then use registration time
from user registrations table - to identify recent low intensity users*/

                                                                       Combined_Active_Subscribers AS (
             SELECT reporting.user_registrations.user_id,
                    Combined_Start_Date,
                    Combined_End_Date,
                    Reporting_End_Date,
                    Subscribers.Reporting_Months,
                    organisation_id,
                    reporting.user_registrations.registration_time                                        AS User_Registration_Date,

                    CASE
                        WHEN last_active_date IS NOT NULL THEN last_active_date
                        ELSE reporting.user_registrations.registration_time END                           AS Combined_Last_Time,
                    DATEDIFF(day, CASE
                                      WHEN last_active_date IS NOT NULL THEN last_active_date
                                      ELSE reporting.user_registrations.registration_time END,
                             Reporting_End_Date)                                                                   AS Days_Since_Login,

                    CASE WHEN Total_Reading_Days IS NULL THEN 0 ELSE Total_Reading_Days END               AS Total_Reading_Days,
                    CASE WHEN Total_Mins_Read IS NULL THEN 0 ELSE Total_Mins_Read END                     AS Total_Mins_Read,
                    CASE
                        WHEN AVG_Month_Reading_Days IS NULL THEN 0
                        ELSE AVG_Month_Reading_Days END                                                   AS AVG_Month_Reading_Days,
                    CASE WHEN AVG_Month_Mins_Read IS NULL THEN 0 ELSE AVG_Month_Mins_Read END             AS AVG_Month_Mins_Read,

                    CASE WHEN user_registrations.organisation_id IS NULL THEN 'B2C' ELSE 'B2B' END        AS User_Group

             FROM reporting.user_registrations
                      LEFT JOIN Recorded_Tracking_Dates
                                on Recorded_Tracking_Dates.user_id = reporting.user_registrations.user_id
                      LEFT JOIN reporting.user_summary
                                on reporting.user_summary.user_id = reporting.user_registrations.user_id
                      LEFT JOIN Reading_Activity_Average
                                On Reading_Activity_Average.user_id = reporting.user_registrations.user_id
                      INNER JOIN Subscribers ON Subscribers.user_id = reporting.user_registrations.user_id
             WHERE Combined_End_Date >= '2022-02-01'
               AND internal_user = 'N'),

/* Define Recency / Frequency & Intensity Levels */

                                                                       Define_R_F_I_Levels AS (
             SELECT Combined_Active_Subscribers.*,

                    CASE
                        WHEN Days_Since_Login <= 7 THEN 'A'
                        WHEN Days_Since_Login > 7 AND Days_Since_Login <= 30 THEN 'B'
                        WHEN Days_Since_Login > 30 AND Days_Since_Login <= 90 THEN 'C'
                        WHEN Days_Since_Login > 90 THEN 'D'
                        ELSE NULL END AS Recency_Level,

                    CASE
                        WHEN Days_Since_Login <= 7 THEN 4
                        WHEN Days_Since_Login > 7 AND Days_Since_Login <= 30 THEN 3
                        WHEN Days_Since_Login > 30 AND Days_Since_Login <= 90 THEN 2
                        WHEN Days_Since_Login > 90 THEN 1
                        ELSE NULL END AS Recency_Score,

                    CASE
                        WHEN Days_Since_Login <= 7 THEN '< One Week Since Last Login'
                        WHEN Days_Since_Login > 7 AND Days_Since_Login <= 30 THEN '< One Month Since Last Login'
                        WHEN Days_Since_Login > 30 AND Days_Since_Login <= 90 THEN '< Three Months Since Last Login'
                        WHEN Days_Since_Login > 90 THEN 'Three Months+ Since Last Login'
                        ELSE NULL END AS Recency_Desc,

                    CASE
                        WHEN AVG_Month_Reading_Days >= 5 THEN 'A'
                        WHEN AVG_Month_Reading_Days >= 2 AND AVG_Month_Reading_Days < 5 THEN 'B'
                        WHEN AVG_Month_Reading_Days > 0 AND AVG_Month_Reading_Days <= 2 THEN 'C'
                        WHEN AVG_Month_Reading_Days = 0 THEN 'D'
                        ELSE NULL END AS Frequency_Level,

                    CASE
                        WHEN AVG_Month_Reading_Days >= 5 THEN 4
                        WHEN AVG_Month_Reading_Days >= 2 AND AVG_Month_Reading_Days < 5 THEN 3
                        WHEN AVG_Month_Reading_Days > 0 AND AVG_Month_Reading_Days <= 2 THEN 2
                        WHEN AVG_Month_Reading_Days = 0 THEN 1
                        ELSE NULL END AS Frequency_Score,

                    CASE
                        WHEN AVG_Month_Reading_Days >= 5 THEN '5+ AVG Active Reading Days Per Month'
                        WHEN AVG_Month_Reading_Days >= 2 AND AVG_Month_Reading_Days < 5
                            THEN '2-5 AVG Active Reading Days Per Month'
                        WHEN AVG_Month_Reading_Days > 0 AND AVG_Month_Reading_Days <= 2
                            THEN '< 2 AVG Active Reading Days Per Month'
                        WHEN AVG_Month_Reading_Days = 0 THEN '0 AVG Active Reading Days Per Month'
                        ELSE NULL END AS Frequency_Desc,

                    CASE
                        WHEN AVG_Month_Mins_Read >= 60 THEN 'A'
                        WHEN AVG_Month_Mins_Read >= 10 AND AVG_Month_Mins_Read < 60 THEN 'B'
                        WHEN AVG_Month_Mins_Read > 0 AND AVG_Month_Mins_Read <= 10 THEN 'C'
                        WHEN AVG_Month_Mins_Read = 0 THEN 'D'
                        ELSE NULL END AS Intensity_Level,

                    CASE
                        WHEN AVG_Month_Mins_Read >= 60 THEN 4
                        WHEN AVG_Month_Mins_Read >= 10 AND AVG_Month_Mins_Read < 60 THEN 3
                        WHEN AVG_Month_Mins_Read > 0 AND AVG_Month_Mins_Read <= 10 THEN 2
                        WHEN AVG_Month_Mins_Read = 0 THEN 1
                        ELSE NULL END AS Intensity_Score,

                    CASE
                        WHEN AVG_Month_Mins_Read >= 60 THEN '60+ AVG Reading Minutes Per Month'
                        WHEN AVG_Month_Mins_Read >= 10 AND AVG_Month_Mins_Read < 60
                            THEN '10 - 60 AVG Reading Minutes Per Month'
                        WHEN AVG_Month_Mins_Read > 0 AND AVG_Month_Mins_Read <= 10
                            THEN '< 10 AVG Reading Minutes Per Month'
                        WHEN AVG_Month_Mins_Read = 0 THEN '0 AVG Reading Minutes Per Month'
                        ELSE NULL END AS Intensity_Desc

             FROM Combined_Active_Subscribers)

    /* Combine levels & scores. Also add descriptions to make it easier for other users */

SELECT user_id, Combined_Start_Date, Reporting_End_Date, Days_Since_Login, AVG_Month_Reading_Days,
                                                                       AVG_Month_Mins_Read, User_Group,
    (Recency_Score + Frequency_Score + Intensity_Score) AS Combined_Score, (
SELECT MIN (Reporting_End_Date)
FROM Define_R_F_I_Levels) AS Reporting_History_Date,
    (Recency_Level + Frequency_Level + Intensity_Level) AS Combined_Cohort
FROM Define_R_F_I_Levels

    )




