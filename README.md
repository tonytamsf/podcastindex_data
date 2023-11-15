cat extract.sql|  sqlite3 podcastindex_feeds.db > all_urls.txt

awk -F '|' '{print $2}' all_urls.txt > urls.txt
select count(*) from urls ;
