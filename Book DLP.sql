
/*Insert values into book price table  */

insert into reporting_layer.book_dlp (
    SELECT id                AS Book_Id,
           CASE
               WHEN gbp_price_exvat_float IS NOT NULL THEN gbp_price_exvat_float
               WHEN gbp_price_exvat_float IS NULL AND usd_price_exvat_float IS NOT NULL
                   THEN usd_price_exvat_float / 1.25
               WHEN gbp_price_exvat_float IS NULL AND usd_price_exvat_float IS NULL
                   AND eur_price_exvat_float IS NOT NULL THEN usd_price_exvat_float / 1.15
               ELSE NULL END AS dlp
    FROM prod.book_meta
)