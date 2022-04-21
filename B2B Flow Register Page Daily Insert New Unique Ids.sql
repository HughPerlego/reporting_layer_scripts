
insert into reporting_layer.b2b_group_register_summary (

WITH Group_Register_URL AS (
    SELECT *
    FROM prod.event_tracking
    WHERE current_url LIKE 'https://www.perlego.com/group-register?orgt1=%'),

/* find urls group register pages accessed, filter to organisation code as well - mark organisation number as some seem to have signed up with the org number instead */
Group_Register AS (
SELECT unique_id, POSITION('orgt1' IN current_url) AS position_find, MIN(record_date) as first_recorded_date,
       SUBSTRING(current_url, position('orgt1=' IN current_url) + 6, LEN(current_url) - POSITION('orgt1' IN current_url)) AS Organisation_code
FROM Group_Register_URL
WHERE Group_Register_URL.unique_id not in (select b2b_group_register_summary.unique_id from b2b_group_register_summary)
GROUP BY unique_id, position('orgt1' IN current_url), SUBSTRING(current_url, position('orgt1=' IN current_url) + 6, LEN(current_url) -  POSITION('orgt1' IN current_url))),

/* filter group register to only include records past the last recorded date in the b2b_group_register_summary */

Group_Register_Filtered AS (
SELECT *
FROM Group_Register
WHERE first_recorded_date >= (select max(first_recorded_date) from reporting_layer.b2b_group_register_summary)),

/* order unique ids by first_recorded_date to filter out duplicates / times recorded twice in quick succession */

Mark_first_record AS (
SELECT *, row_number() over (partition by unique_id order by first_recorded_date) as unique_id_row_number
FROM Group_Register_Filtered),

Filtered_Group_Register AS (
SELECT *
FROM Mark_first_record
WHERE unique_id_row_number = 1),

/* mark codes - if organisation code below 3 - mark as possible organisation id  */
Combined_Table AS (
SELECT Filtered_Group_Register.*,
       CASE WHEN reporting_layer.b2b_organisation_hashes.organisation_id IS NULL AND len(Filtered_Group_Register.Organisation_code) <= 3 AND left(Filtered_Group_Register.Organisation_code,4) between 0 and 9999
           THEN CAST(Filtered_Group_Register.Organisation_code AS INT) ELSE reporting_layer.b2b_organisation_hashes.organisation_id END AS Calculated_OrganisationID
FROM Filtered_Group_Register
LEFT JOIN reporting_layer.b2b_organisation_hashes ON TRIM(reporting_layer.b2b_organisation_hashes.hash) = Filtered_Group_Register.Organisation_code),

/* B2B / B2C Signup */
First_User_Actions AS (
SELECT unique_id, first_value(event_name) OVER (PARTITION BY unique_id ORDER BY record_date ROWS BETWEEN UNBOUNDED preceding and unbounded  following ) AS First_User_Event,
       first_value(user_agent) OVER (PARTITION BY unique_id ORDER BY record_date ROWS BETWEEN UNBOUNDED preceding and unbounded  following ) AS First_User_Agent
FROM prod.event_tracking
WHERE record_date >= (select max(first_recorded_date) from reporting_layer.b2b_group_register_summary)
AND unique_id IN (SELECT unique_id FROM Combined_Table)
AND user_id IS NOT NULL),

First_User_Actions_grouped AS (
SELECT unique_id, First_User_Event, First_User_Agent
FROM First_User_Actions
GROUP BY unique_id, First_User_Event, First_User_Agent),

/* Full Table */

Full_Table AS (
SELECT Combined_Table.*, First_User_Actions_grouped.First_User_Event, First_User_Agent
FROM Combined_Table
LEFT JOIN First_User_Actions_grouped ON First_User_Actions_grouped.unique_id = Combined_Table.unique_id),

/* Clicked On other pages after link */
Other_Pages_Data AS (
SELECT *, ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY record_date ASC) AS unique_row_number,
       CASE WHEN current_url LIKE '%group-register%' THEN 1 else 0 end as group_register_page,
       CASE WHEN user_agent IS NOT NULL THEN
        first_value(user_agent) OVER (PARTITION BY unique_id ORDER BY record_date ROWS BETWEEN UNBOUNDED preceding and unbounded  following) ELSE NULL END AS First_User_Agent_NoJoin,
       CASE WHEN event_tracking.environment_name IS NOT NULL THEN
       first_value(environment_name) OVER (PARTITION BY unique_id ORDER BY record_date ROWS BETWEEN UNBOUNDED preceding and unbounded  following) ELSE NULL END AS First_User_enviroment_NoJoin
FROM prod.event_tracking
WHERE unique_id IN (SELECT unique_id
                    FROM Full_Table
                    WHERE First_User_Event IS NULL)),

first_hit_register_page as (
SELECT unique_id, min(unique_row_number) as first_group_register_page_num
FROM Other_Pages_Data
WHERE group_register_page = 1
group by unique_id),

/* Mark actions pre & post register page - Case statements  */
Action_Pre_Post_Register_Page AS (
SELECT Other_Pages_Data.*, first_group_register_page_num
FROM Other_Pages_Data
LEFT JOIN first_hit_register_page ON first_hit_register_page.unique_id = Other_Pages_Data.unique_id
),

/* split into pre & post reading  */

Pre_Register_Page AS (
SELECT unique_id, First_User_Agent_NoJoin, First_User_enviroment_NoJoin, max(unique_row_number) AS Events_Pre_Register,
          MAX(CASE WHEN current_url IN ('https://www.perlego.com/', 'https://www.perlego.com/home') THEN 1 else 0 end) as Homepage_Pre,
          MAX(CASE WHEN current_url LIKE 'https://www.perlego.com/search' THEN 1 else 0 end) as Search_Pre,
          MAX(CASE WHEN current_url = 'https://www.perlego.com/pricing' THEN 1 else 0 end) as Pricing_Pre,
          MAX(CASE WHEN current_url = 'https://www.perlego.com/login' THEN 1 else 0 end) as Login_Pre,
          MAX(CASE WHEN current_url = 'https://www.perlego.com/sign-up' THEN 1 else 0 end) as SignUP_Pre,
          MAX(CASE WHEN current_url = 'https://www.perlego.com/mission' THEN 1 else 0 end) as Mission_Pre
FROM Action_Pre_Post_Register_Page
WHERE unique_row_number < first_group_register_page_num
GROUP BY unique_id, First_User_Agent_NoJoin, First_User_enviroment_NoJoin),

Post_Register_Page AS (
SELECT unique_id, First_User_Agent_NoJoin,First_User_enviroment_NoJoin,  max(unique_row_number) AS Events_Post_Register,
          MAX(CASE WHEN current_url IN ('https://www.perlego.com/', 'https://www.perlego.com/home') THEN 1 else 0 end) as Homepage_Post,
          MAX(CASE WHEN current_url LIKE 'https://www.perlego.com/search' THEN 1 else 0 end) as Search_Post,
          MAX(CASE WHEN current_url = 'https://www.perlego.com/pricing' THEN 1 else 0 end) as Pricing_Post,
          MAX(CASE WHEN current_url = 'https://www.perlego.com/login' THEN 1 else 0 end) as Login_Post,
          MAX(CASE WHEN current_url = 'https://www.perlego.com/sign-up' THEN 1 else 0 end) as SignUp_Post,
          MAX(CASE WHEN current_url = 'https://www.perlego.com/mission' THEN 1 else 0 end) as Mission_Post
FROM Action_Pre_Post_Register_Page
WHERE unique_row_number > first_group_register_page_num
GROUP BY unique_id, First_User_Agent_NoJoin, First_User_enviroment_NoJoin),

/* Past group register page */
action_past_register_page AS (
SELECT *
FROM Action_Pre_Post_Register_Page
WHERE unique_row_number >= first_group_register_page_num),

/* summarise results - 1 count of unique id means they left straight away once getting to page
   count > 1 they clicked around afterwards
   1 - first action was clicking link to group register page
   >1 on site before group register page

Also take max value of case statements to see if user hit certain pages e.g. pricing pre group register page - not
looking at total count for now*/

Summarised_action_list as (
SELECT unique_id,
       First_User_Agent_NoJoin,
       First_User_enviroment_NoJoin,
       COUNT(*) AS User_Actions,
       MIN(unique_row_number) AS Action_Number
FROM action_past_register_page
GROUP BY unique_id, First_User_Agent_NoJoin, First_User_enviroment_NoJoin),

Case_Statements_List AS (
SELECT Summarised_action_list.unique_id, Summarised_action_list.First_User_Agent_NoJoin, Summarised_action_list.First_User_enviroment_NoJoin, Homepage_Pre, Search_Pre, Search_Post, Pricing_Pre, Login_Pre, SignUP_Pre, Mission_Pre, Homepage_Post, Pricing_Post, Login_Post,
       SignUp_Post, Mission_Post, Events_Pre_Register, Events_Post_Register,
       CASE WHEN User_Actions = 1 THEN 1 ELSE 0 END AS Inmmediately_Left_Page,
       CASE WHEN User_Actions > 1 THEN 1 ELSE 0 END AS Clicked_Around,
       CASE WHEN Action_Number = 1 THEN 1 ELSE 0 END AS First_Action_Group_Register,
       CASE WHEN Action_Number > 1 THEN 1 ELSE 0 END AS First_Action_On_Perlego
FROM Summarised_action_list
LEFT JOIN Pre_Register_Page ON Pre_Register_Page.unique_id = Summarised_action_list.unique_id
LEFT JOIN Post_Register_Page ON Post_Register_Page.unique_id = Summarised_action_list.unique_id),

/* rejoin to main data table */

Rejoined_Main_Table AS (
SELECT Full_Table.*, First_User_Agent_NoJoin, First_User_enviroment_NoJoin, Homepage_Pre, Search_Pre, Pricing_Pre, Login_Pre, SignUP_Pre, Mission_Pre, Homepage_Post, Search_Post, Pricing_Post, Login_Post,
       SignUp_Post, Mission_Post, Events_Pre_Register, Events_Post_Register,
       Inmmediately_Left_Page, Clicked_Around, First_Action_Group_Register, First_Action_On_Perlego, NULL as user_id
FROM Full_Table
LEFT JOIN Case_Statements_List ON Case_Statements_List.unique_id = Full_Table.unique_id),


/* B2C Signed Up - Then Moved */
B2c_Sign_Up_First AS (
SELECT NULL as unique_id,
       NULL as position_find,
       NULL as first_recorded_date,
       NULL as organisation_code,
       NULL as unique_row_number,
       organisation_id as calculated_organisationid,
       NULL as first_user_event,
       NULL as first_user_agent,
       NULL as First_User_enviroment_NoJoin,
       NULL as First_User_Agent_NoJoin,
       NULL as Homepage_Pre,
       NULL as Search_Pre,
       NULL as Pricing_Pre,
       NULL as Login_Pre,
       NULL as SignUP_Pre,
       NULL as Mission_Pre,
       NULL as Homepage_Post,
       NULL as Search_Post,
       NULL as Pricing_Post,
       NULL as Login_Post,
       NULL as SignUp_Post,
       NULL as Mission_Post,
       NULL AS Events_Pre_Register,
       NULL AS Events_Post_Register,
       NULL as immediately_left_page,
       NULL as clicked_around,
       NULL as first_action_group_register,
       NULL as first_action_on_perlego,
       user_id
FROM reporting.user_registrations
WHERE payment_channel_crude = 'indirect'
and user_id in (select user_id from prod.event_tracking where event_name = 'account creation')
    and user_id not in (select main_reporting_id from reporting_layer.b2b_group_register_summary)),

Joined_Tables AS (
SELECT *
FROM Rejoined_Main_Table
UNION
SELECT *
FROM B2c_Sign_Up_First),

/* Mark case statemtents to create final table  */

Final_Export_Table AS (
SELECT unique_id,
       CASE WHEN unique_id IS NOT NULL THEN CAST(unique_id as varchar(max)) ELSE CAST(user_id as varchar(max)) END AS Main_Reporting_Id,
       first_recorded_date, Organisation_code, Calculated_OrganisationID, first_user_agent, First_User_Agent_NoJoin, First_User_enviroment_NoJoin,

       CASE WHEN First_User_Event = 'account creation' THEN 'Register Page - Account'
            WHEN First_User_Event <> 'account creation' THEN 'Register Page - B2B Signup'
            WHEN First_User_Event IS NULL AND user_id IS NULL THEN 'Register Page - No Signup'
            WHEN user_id IS NOT NULL THEN 'Register Page - B2C Transfered To B2B'

           ELSE NULL END AS User_Status,

       Inmmediately_Left_Page AS NoSignup_Left_Registration_Page,
       Clicked_Around AS NoSignup_Another_Page,

       Homepage_Pre, Search_Pre, Pricing_Pre, Login_Pre, SignUP_Pre, Mission_Pre, Homepage_Post, Search_Post, Pricing_Post, Login_Post,
       SignUp_Post, Mission_Post, Events_Pre_Register, Events_Post_Register

FROM Joined_Tables
WHERE Calculated_OrganisationID IS NOT NULL),

/* Total group register page count */

     register_count AS (
SELECT event_tracking.unique_id, count(*) AS total_register_page
FROM prod.event_tracking
WHERE event_tracking.unique_id IN (SELECT Final_Export_Table.unique_id FROM Final_Export_Table)
AND current_url LIKE '%group-register%'
GROUP BY event_tracking.unique_id)


/* Final Result  */

SELECT Final_Export_Table.unique_id,
       Main_Reporting_Id,
       first_recorded_date,
       Organisation_code,
       Calculated_OrganisationID,
       User_Status,
       NoSignup_Left_Registration_Page,
       NoSignup_Another_Page,
       Homepage_Pre,
       Search_Pre,
       Pricing_Pre,
       Login_Pre,
       SignUP_Pre,
       Mission_Pre,
       Homepage_Post,
       Search_Post,
       Pricing_Post,
       Login_Post,
       SignUp_Post,
       Mission_Post,
       Events_Pre_Register,
       Events_Post_Register,
       total_register_page AS group_register_page_count
FROM Final_Export_Table
LEFT JOIN register_count ON register_count.unique_id = Final_Export_Table.unique_id)





