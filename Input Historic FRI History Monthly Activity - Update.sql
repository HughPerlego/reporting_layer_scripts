INSERT INTO "perlego-analytics-prod".reporting_layer.user_rfi_history_subscriber_monthly (

/* Define Cut off date used in reading calculations and earliest acceptable subscription date -
   marked in table below so you don't have to filter the tables individually later on in the query
   Making it cleaner to update historic data*/

    WITH Parameter_Fields AS (

    SELECT user_id,
           CAST('2022-02-01' AS Date) AS Reporting_Start_Date,
           CAST('2022-02-28' AS Date) AS Subscribed_After_Date,
           30 AS Days_From_cutoff
    FROM reporting.subscriptions
    WHERE reporting.subscriptions.subscription_type = 'subscribed'
    AND start_timestamp >= '2017-01-01'
    GROUP BY user_id, CAST('2022-02-01' AS Date), CAST('2022-02-28' AS Date)
),

/* Filter to subscribers from 2017 onwards - define start and end dates depending on payment channel
   for B2C give priority to subscriber who has chosen to cancel - as in effect they are already churrned*/

    Subscribe_Change AS (
        SELECT reporting.subscriptions.*,
               CASE
                   WHEN payment_channel = 'direct' AND paid_start_timestamp IS NOT NULL THEN paid_start_timestamp
                   ELSE start_timestamp END AS Combined_Start_Date,
               CASE
                   WHEN payment_channel = 'direct' AND subscription_canceled_at IS NOT NULL THEN subscription_canceled_at
                        WHEN subscription_canceled_at IS NULL AND paid_end_timestamp IS NOT NULL THEN paid_end_timestamp
                   ELSE end_timestamp END AS Combined_End_Date
        FROM reporting.subscriptions
        WHERE reporting.subscriptions.subscription_type = 'subscribed'
        AND start_timestamp >= '2017-01-01'
        AND subscription_canceled_at IS NULL),

    /* Limit to subscribers who have been subscribed at least more than 30 days after the cut off date*/

    Filter_Subscribers AS (
     SELECT Subscribe_Change.*, Reporting_Start_Date, Subscribed_After_Date,
            DATEDIFF(day, Combined_Start_Date, Subscribed_After_Date) as Subscribed_days_from_cutoff
     FROM Subscribe_Change
     LEFT JOIN Parameter_Fields ON Parameter_Fields.user_id = Subscribe_Change.user_id
     WHERE Combined_End_Date >= Subscribed_After_Date
     AND DATEDIFF(day, Combined_Start_Date, Subscribed_After_Date) >= Days_From_cutoff
     ORDER BY Combined_Start_Date DESC),

    /* Last login calculation - work out last session times by user id */
    Recorded_Tracking_Dates AS (
      select user_id, max(cast(event_timestamp as date)) as last_active_date
      from reporting.user_tracking_events
      where event_name not in ('payment failed', 'payment recieved', 'payment refunded', 'page close')
      and user_id in (select user_id from Filter_Subscribers where event_timestamp <= Subscribed_After_Date)
      group by user_id),

/* Frequency Calculation
 Count active reading days per user & seconds read - within reporting period
   Only include those with duration above 0 - so had to stay on the page for five seconds or longer*/

    Reading_Activity AS (
             SELECT reporting.reading_activity_daily.user_id,
                    1.0 * COUNT(DISTINCT (cast(reading_date AS DATE))) AS Active_Reading_Days
             FROM reporting.reading_activity_daily
             INNER JOIN Filter_Subscribers ON Filter_Subscribers.user_id = reporting.reading_activity_daily.user_id
             WHERE reading_date >= Reporting_Start_Date AND reading_date <= Subscribed_After_Date
             GROUP BY reporting.reading_activity_daily.user_id),

/* Session Times - remove backend events and record min max time of each user session */
    Session_Times AS (
         SELECT user_id, session_id,
                MIN(event_timestamp) AS First_Session_Time,
                MAX(event_timestamp) AS Last_Session_Time
         FROM reporting.user_tracking_events
         WHERE event_name not in ('payment failed', 'payment recieved', 'payment refunded', 'page close')
           AND session_id IS NOT NULL and session_id <> ''
         AND user_id IN (SELECT user_id
                         FROM Filter_Subscribers
                         WHERE event_timestamp <= Subscribed_After_Date AND event_timestamp >= Reporting_Start_Date)
         GROUP BY user_id, session_id
     ),
    Session_Duration AS (
    SELECT user_id, SUM(DATEDIFF(secs, First_Session_Time, Last_Session_Time))  AS Total_Seconds_All_Sessions
    FROM Session_Times
    GROUP BY user_id),

/* Join RFI measures to Filter_Subscribers table - also join to user registrations to identify B2C / B2B users and
   remove internal ones*/

    Join_Measure_Tables_Together AS (
    SELECT Filter_Subscribers.*, last_active_date,
           CASE WHEN Active_Reading_Days IS NULL THEN 0 ELSE CAST(Active_Reading_Days AS INT) END AS Active_Reading_Days,
           CASE WHEN Total_Seconds_All_Sessions IS NULL THEN 0 ELSE CAST(Total_Seconds_All_Sessions / 60 AS INT) END AS Session_Total_Minutes,
    CASE WHEN last_active_date IS NULL THEN CAST(DATEDIFF(days, Combined_Start_Date, Subscribed_After_Date) AS INT) ELSE
        CAST(DATEDIFF(days, last_active_date, Subscribed_After_Date) AS INT) END AS Days_Since_login,
    CASE WHEN reporting.user_registrations.organisation_id IS NOT NULL THEN 'B2B' ELSE 'B2C' END AS User_Group
    FROM Filter_Subscribers
    LEFT JOIN reporting.user_registrations on reporting.user_registrations.user_id = Filter_Subscribers.user_id
    LEFT JOIN Recorded_Tracking_Dates ON Recorded_Tracking_Dates.user_id = Filter_Subscribers.user_id
    LEFT JOIN Reading_Activity ON Reading_Activity.user_id = Filter_Subscribers.user_id
    LEFT JOIN Session_Duration ON Session_Duration.user_id = Filter_Subscribers.user_id
    ),

/* Define RFI Measure Levels */
    Define_R_F_I_Levels AS (
 SELECT Join_Measure_Tables_Together.*,
        CASE
            WHEN Days_Since_login <= 7 THEN 'A'
            WHEN Days_Since_login > 7 AND Days_Since_login <= 30 THEN 'B'
            WHEN Days_Since_login > 30 AND Days_Since_login <= 90 THEN 'C'
            WHEN Days_Since_login > 90 THEN 'D'
            ELSE NULL END AS Recency_Level,

          CASE
              WHEN Days_Since_login <= 7 THEN 4
              WHEN Days_Since_login > 7 AND Days_Since_login <= 30 THEN 3
              WHEN Days_Since_login > 30 AND Days_Since_login <= 90 THEN 2
              WHEN Days_Since_login > 90 THEN 1
              ELSE NULL END AS Recency_Score,

        CASE
            WHEN Active_Reading_Days >= 5 THEN 'A'
            WHEN Active_Reading_Days >= 2 AND Active_Reading_Days < 5 THEN 'B'
            WHEN Active_Reading_Days > 0 AND Active_Reading_Days <= 2 THEN 'C'
            WHEN Active_Reading_Days = 0 is null THEN 'D'
            ELSE NULL END AS Frequency_Level,

         CASE
             WHEN Active_Reading_Days >= 5 THEN 4
             WHEN Active_Reading_Days >= 2 AND Active_Reading_Days < 5 THEN 3
             WHEN Active_Reading_Days > 0 AND Active_Reading_Days <= 2 THEN 2
             WHEN Active_Reading_Days = 0 THEN 1
             ELSE NULL END AS Frequency_Score,

        CASE
            WHEN Session_Total_Minutes >= 60 THEN 'A'
            WHEN Session_Total_Minutes >= 10 AND Session_Total_Minutes < 60 THEN 'B'
            WHEN Session_Total_Minutes > 0 AND Session_Total_Minutes <= 10 THEN 'C'
            WHEN Session_Total_Minutes = 0 THEN 'D'
            ELSE NULL END AS Intensity_Level,

        CASE
            WHEN Session_Total_Minutes >= 60 THEN 4
            WHEN Session_Total_Minutes >= 10 AND Session_Total_Minutes < 60 THEN 3
            WHEN Session_Total_Minutes > 0 AND Session_Total_Minutes <= 10 THEN 2
            WHEN Session_Total_Minutes = 0 THEN 1
            ELSE NULL END AS Intensity_Score

 FROM Join_Measure_Tables_Together)

/* Combine Results */
SELECT Define_R_F_I_Levels.user_id, Combined_Start_Date, Combined_End_Date,
    Days_Since_Login, Active_Reading_Days, Session_Total_Minutes,
    CAST((Recency_Score + Frequency_Score + Intensity_Score) as int) AS Combined_Score,
    Reporting_Start_Date, User_Group, (Recency_Level + Frequency_Level + Intensity_Level) AS Combined_Cohort

FROM Define_R_F_I_Levels
    )


