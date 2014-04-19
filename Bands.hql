-- hive  --hivevar reference=base --hivevar input=sum --hivevar out_schema=varcham -f Bands.hql

-- This HQL script is meant to match a list of artist plays with a base reference list of Artist names

-- QUESTIONS:
-- Is this data in a schema? Which schema?
-- Is the file comma delimited?
-- Assuming the files are stored in /data/.. folder in HDFS

--
--                   ..oo$00ooo..                    ..ooo00$oo..
--                .o$$$$$$$$$'                          '$$$$$$$$$o.
--             .o$$$$$$$$$"             .   .              "$$$$$$$$$o.
--           .o$$$$$$$$$$~             /$   $\              ~$$$$$$$$$$o.
--         .{$$$$$$$$$$$.              $\___/$               .$$$$$$$$$$$}.
--        o$$$$$$$$$$$$8              .$$$$$$$.               8$$$$$$$$$$$$o
--       $$$$$$$$$$$$$$$              $$$$$$$$$               $$$$$$$$$$$$$$$
--      o$$$$$$$$$$$$$$$.             o$$$$$$$o              .$$$$$$$$$$$$$$$o
--      $$$$$$$$$$$$$$$$$.           o{$$$$$$$}o            .$$$$$$$$$$$$$$$$$
--     ^$$$$$$$$$$$$$$$$$$.         J$$$$$$$$$$$L          .$$$$$$$$$$$$$$$$$$^
--     !$$$$$$$$$$$$$$$$$$$$oo..oo$$$$$$$$$$$$$$$$$oo..oo$$$$$$$$$$$$$$$$$$$$$!
--     {$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$}
--     6$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$?
--     '$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$'
--      o$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$o
--       $$$$$$$$$$$$$$;'~`^Y$$$7^''o$$$$$$$$$$$o''^Y$$$7^`~';$$$$$$$$$$$$$$$
--       '$$$$$$$$$$$'       `$'    `'$$$$$$$$$'     `$'       '$$$$$$$$$$$$'
--        !$$$$$$$$$7         !       '$$$$$$$'       !         V$$$$$$$$$!
--         ^o$$$$$$!                   '$$$$$'                   !$$$$$$o^
--           ^$$$$$"                    $$$$$                    "$$$$$^
--             'o$$$`                   ^$$$'                   '$$$o'
--               ~$$$.                   $$$.                  .$$$~
--                 '$;.                  `$'                  .;$'
--                    '.                  !                  .`
--      
--                 
--  


-- Import the artist file

!echo 'Importing Artist Reference List'; 
drop table if exists ${out_schema}.${reference};

CREATE TABLE ${out_schema}.${reference}(artist STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

load data local inpath '/home/varchambault/${reference}.txt' into table ${out_schema}.${reference};

!echo 'Importing Plays List'; 
drop table if exists ${out_schema}.${input};

CREATE TABLE ${out_schema}.${input}(artist_play STRING,stamp INT,plays INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

load data local inpath '/home/varchambault/${input}.txt' into table ${out_schema}.${input};




-- Merge tables into new super-table!

!echo 'Joining on name match'; 
drop table if exists ${out_schema}.${reference}_compare_${input};

CREATE TABLE ${out_schema}.${reference}_compare_${input} as 
SELECT ${reference}.artist, sum(${input}.plays) as total_plays 
FROM ${out_schema}.${input} JOIN ${out_schema}.${reference} 
WHERE (CONCAT("%",${input}.artist_play,"%") RLIKE CONCAT("%",${reference}.artist,"%") 
	or CONCAT("%",${reference}.artist,"%") RLIKE CONCAT("The ",${input}.artist_play,"%") 
	or CONCAT("%",${input}.artist_play,"%") RLIKE CONCAT("The ",${reference}.artist,"%") 
	or CONCAT("%",${reference}.artist,",The%") RLIKE CONCAT("%The ",${input}.artist_play,"%") 
	or REVERSE(${reference}.artist) RLIKE REVERSE(${input}.artist_play)) 
GROUP BY ${reference}.artist;

-- Turn on column headers

set hive.cli.print.header=true;

-- Display results

select * from ${out_schema}.${reference}_compare_${input};

!echo 'Process complete...have a great day!'