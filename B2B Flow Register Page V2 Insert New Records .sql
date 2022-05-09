WITH Group_Register_URL AS (
    SELECT *
    FROM prod.event_tracking
    WHERE current_url LIKE 'https://www.perlego.com/group-register?orgt1=%'),

Group_Register AS (
SELECT unique_id, POSITION('orgt1' IN current_url) AS position_find, MIN(record_date) as first_recorded_date,
       SUBSTRING(current_url, position('orgt1=' IN current_url) + 6, LEN(current_url) - POSITION('orgt1' IN current_url)) AS Organisation_code
FROM prod.event_tracking
WHERE  Group_Register_URL.unique_id not in (select reporting_layer.b2b_group_register_summary.unique_id from reporting_layer.b2b_group_register_summary)
GROUP BY unique_id, position('orgt1' IN current_url), SUBSTRING(current_url, position('orgt1=' IN current_url) + 6, LEN(current_url) -  POSITION('orgt1' IN current_url))),

    /* mark codes - if organisation code below 3 - mark as possible organisation id  */
Combined_Table AS (
SELECT Group_Register.*,
       CASE WHEN b2b_organisation_hashes.organisation_id IS NULL AND len(Group_Register.Organisation_code) <= 3 AND left(Group_Register.Organisation_code,4) between 0 and 9999
           THEN CAST(Group_Register.Organisation_code AS INT) ELSE b2b_organisation_hashes.organisation_id END AS Calculated_OrganisationID
FROM Group_Register
LEFT JOIN reporting_layer.b2b_organisation_hashes ON b2b_organisation_hashes.hash = Group_Register.Organisation_code),

/* Remove records from organisations we are not recording  */

Combined_Table_Filter_Organisation AS (
select *, MIN(first_recorded_date) over (partition by unique_id) AS Unique_id_first_date
FROM Combined_Table
WHERE calculated_organisationid NOT IN (50, 51, 6, 1, 7, 90, 55, 98, 197, 58, 99, 92, 188, 204, 253, 28, 148, 187, 186, 48)),

     /* filter out duplicates - keep the first time the unique id appeared  */
Combined_Table_Filter_user AS (
SELECT *
FROM Combined_Table_Filter_Organisation
WHERE first_recorded_date = Unique_id_first_date),

/* B2B / B2C Signup */

First_User_Actions AS (
SELECT unique_id, first_value(event_name) OVER (PARTITION BY unique_id ORDER BY record_date ROWS BETWEEN UNBOUNDED preceding and unbounded  following ) AS First_User_Event
FROM prod.event_tracking
WHERE unique_id IN (SELECT unique_id FROM Combined_Table_Filter_user)
AND user_id IS NOT NULL),

First_User_Actions_grouped AS (
SELECT unique_id, First_User_Event
FROM First_User_Actions
GROUP BY unique_id, First_User_Event),

/* Full Table */
Full_Table AS (
SELECT Combined_Table_Filter_user.*, First_User_Actions_grouped.First_User_Event
FROM Combined_Table_Filter_user
LEFT JOIN First_User_Actions_grouped ON First_User_Actions_grouped.unique_id = Combined_Table_Filter_user.unique_id),

/* Clicked On other pages after link - limit to pages of interest to reduce data when data is partiitioned   */
Other_Pages_Data_No_Signup AS (
SELECT unique_id, current_url, record_date,
       CASE WHEN current_url LIKE '%group-register%' THEN 1 else 0 end as group_register_page,
       CASE WHEN current_url LIKE '%group-register%' THEN record_date else NULL end as group_register_date
FROM prod.event_tracking
WHERE unique_id IN (SELECT unique_id
                    FROM Full_Table
                    WHERE First_User_Event IS NULL)),

  /* First group register date */
First_Register_Date_Table AS (
SELECT unique_id, MIN(group_register_date) AS First_Group_Page_Date
FROM Other_Pages_Data_No_Signup
GROUP BY unique_id),

Other_Pages_Data_No_Signup_GroupPage AS (
    SELECT Other_Pages_Data_No_Signup.*, First_Group_Page_Date
    FROM Other_Pages_Data_No_Signup
    LEFT JOIN First_Register_Date_Table ON First_Register_Date_Table.unique_id = Other_Pages_Data_No_Signup.unique_id
),

/* split into pre & post reading  */

Pre_Register_Page AS (
SELECT unique_id,
          MAX(CASE WHEN current_url IN ('https://www.perlego.com/', 'https://www.perlego.com/home') THEN 1 else 0 end) as Homepage_Pre,
          MAX(CASE WHEN current_url LIKE 'https://www.perlego.com/search' THEN 1 else 0 end) as Search_Pre,
          MAX(CASE WHEN current_url = 'https://www.perlego.com/pricing' THEN 1 else 0 end) as Pricing_Pre,
          MAX(CASE WHEN current_url = 'https://www.perlego.com/login' THEN 1 else 0 end) as Login_Pre,
          MAX(CASE WHEN current_url = 'https://www.perlego.com/sign-up' THEN 1 else 0 end) as SignUP_Pre,
          MAX(CASE WHEN current_url = 'https://www.perlego.com/mission' THEN 1 else 0 end) as Mission_Pre,
          COUNT(*) AS Events_Pre_Register
FROM Other_Pages_Data_No_Signup_GroupPage
WHERE record_date < First_Group_Page_Date
GROUP BY unique_id),


Post_Register_Page AS (
SELECT unique_id,
          MAX(CASE WHEN current_url IN ('https://www.perlego.com/', 'https://www.perlego.com/home') THEN 1 else 0 end) as Homepage_Post,
          MAX(CASE WHEN current_url LIKE 'https://www.perlego.com/search' THEN 1 else 0 end) as Search_Post,
          MAX(CASE WHEN current_url = 'https://www.perlego.com/pricing' THEN 1 else 0 end) as Pricing_Post,
          MAX(CASE WHEN current_url = 'https://www.perlego.com/login' THEN 1 else 0 end) as Login_Post,
          MAX(CASE WHEN current_url = 'https://www.perlego.com/sign-up' THEN 1 else 0 end) as SignUp_Post,
          MAX(CASE WHEN current_url = 'https://www.perlego.com/mission' THEN 1 else 0 end) as Mission_Post,
          COUNT(*) AS Events_Post_Register
FROM Other_Pages_Data_No_Signup_GroupPage
WHERE record_date > First_Group_Page_Date
GROUP BY unique_id),

/* summarise results - 1 count of unique id means they left straight away once getting to page
   count > 1 they clicked around afterwards
   1 - first action was clicking link to group register page
   >1 on site before group register page

Also take max value of case statements to see if user hit certain pages e.g. pricing pre group register page - not
looking at total count for now*/


Case_Statements_List AS (
SELECT Other_Pages_Data_No_Signup_GroupPage.unique_id, Homepage_Pre, Search_Pre, Search_Post, Pricing_Pre, Login_Pre, SignUP_Pre, Mission_Pre, Homepage_Post, Pricing_Post, Login_Post,
       SignUp_Post, Mission_Post, Events_Pre_Register, Events_Post_Register,
       CASE WHEN Events_Post_Register IS NULL THEN 1 ELSE 0 END AS Inmmediately_Left_Page,
       CASE WHEN Events_Post_Register > 0 THEN 1 ELSE 0 END AS Clicked_Around,
       CASE WHEN Events_Pre_Register IS NULL THEN 1 ELSE 0 END AS First_Action_Group_Register,
       CASE WHEN Events_Pre_Register > 0 THEN 1 ELSE 0 END AS First_Action_On_Perlego
FROM Other_Pages_Data_No_Signup_GroupPage
LEFT JOIN Pre_Register_Page ON Pre_Register_Page.unique_id = Other_Pages_Data_No_Signup_GroupPage.unique_id
LEFT JOIN Post_Register_Page ON Post_Register_Page.unique_id = Other_Pages_Data_No_Signup_GroupPage.unique_id),

/* rejoin to main data table */

Rejoined_Main_Table AS (
SELECT Full_Table.unique_id, first_recorded_date, Organisation_code, Calculated_OrganisationID,
       First_User_Event, Homepage_Pre, Search_Pre, Pricing_Pre, Login_Pre, SignUP_Pre, Mission_Pre, Homepage_Post, Search_Post, Pricing_Post, Login_Post,
       SignUp_Post, Mission_Post, Events_Pre_Register, Events_Post_Register,
       Inmmediately_Left_Page, Clicked_Around, First_Action_Group_Register, First_Action_On_Perlego, NULL as user_id
FROM Full_Table
LEFT JOIN Case_Statements_List ON Case_Statements_List.unique_id = Full_Table.unique_id),

/* B2C Signed Up - Then Moved */
B2c_Sign_Up_First AS (
SELECT NULL as unique_id,
       NULL as first_recorded_date,
       NULL as organisation_code,
       organisation_id as calculated_organisationid,
       NULL as first_user_event,
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
and user_id in (select user_id from prod.event_tracking where event_name = 'account creation')),

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
       first_recorded_date, Organisation_code, Calculated_OrganisationID,

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
LEFT JOIN register_count ON register_count.unique_id = Final_Export_Table.unique_id

