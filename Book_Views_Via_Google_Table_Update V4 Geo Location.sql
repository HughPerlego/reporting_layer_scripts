/* Dataset to large to load in one go - will need to update results monthly

  Mark date range in Events_Within_Range table - will only look at events within those selected dates */

 insert into "perlego-analytics-prod".reporting_layer.book_views_via_google (

/* Record Dates */
     WITH Events_Within_Range AS (
         SELECT *
         FROM prod.event_tracking
         WHERE record_date >= '2021-01-01'
           AND record_date < '2022-05-09'
     ),
          PageViews__Within_Range AS (
              SELECT *
              FROM Events_Within_Range
              WHERE event_name = 'page view'
                AND referrer_url like '%google%'
                AND current_url like '%book%'
                AND user_id IS NULL
                AND unique_id IS NOT NULL
                AND unique_id <> ''
          ),

/* filter to valid JSONS  */
          Valid_JSON AS (
              SELECT *
              FROM PageViews__Within_Range
              WHERE is_valid_json(event_properties) = TRUE),

/* mark books with refer page google - missing user id (before they set up their account) */
          Google_Page_Views AS (
              SELECT unique_id,
                     record_date,
                     geo_location,
                     CAST(record_date as date)                               AS Recorded_Date,
                     json_extract_path_text(event_properties, 'content_ids') AS book_id_extract
              FROM Valid_JSON
              WHERE json_extract_path_text(event_properties, 'content_ids') <> ''),

/* Mark clean book id & first book viewed per unique id  */

          Mark_First_Bookid AS (
              SELECT *,
                     CAST(substring(book_id_extract, 2, len(book_id_extract) - 2) AS INT)                                        AS Clean_BookId,
                     FIRST_VALUE(CAST(substring(book_id_extract, 2, len(book_id_extract) - 2) AS INT))
                     OVER (PARTITION BY unique_id ORDER BY record_date rows between unbounded preceding and unbounded following) AS First_BookId
              FROM Google_Page_Views)

/* group by first book id & date - count number of unique users who first landed to the book */

     SELECT First_BookId                AS Book_Id,
            Recorded_Date               AS Reporting_period_start,
            COUNT(DISTINCT (unique_id)) AS Unique_first_views,
            geo_location
     FROM Mark_First_Bookid
     GROUP BY First_BookId, Recorded_Date, geo_location
 )


