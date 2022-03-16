/* Dataset to large to load in one go - will need to update results monthly

  Mark date range in Events_Within_Range table - will only look at events within those selected dates */

 insert into "perlego-analytics-prod".reporting_layer.book_views_via_google (
/* Record Dates */
     WITH Events_Within_Range AS (
         SELECT *
         FROM prod.event_tracking
         WHERE record_date >= '2022-03-01'
           AND record_date < '2022-03-07'
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
                     event_id,
                     event_properties,
                     json_extract_path_text(event_properties, 'content_ids') AS book_id
              FROM Valid_JSON
              WHERE json_extract_path_text(event_properties, 'content_ids') <> ''),

          grouped_user_book as (
              select unique_id, book_id, min(record_date) as first_book_date
              from Google_Page_Views
              where book_id is not null
              group by unique_id, book_id
          ),

          Assign_row_number AS (
              SELECT *,
                     ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY first_book_date ASC) AS book_view_order,
                     REPLACE(book_id, '[', '')                                               as book_id_clean
              FROM grouped_user_book)


     SELECT CAST(REPLACE(book_id_clean, ']', '') AS INT) as Book_id,
            '2022-03-01'                                 AS reporting_period_start,
            COUNT(DISTINCT (unique_id))                  AS Unique_first_views
     FROM Assign_row_number
     WHERE book_view_order = 1
     GROUP BY REPLACE(book_id_clean, ']', '')
 )

