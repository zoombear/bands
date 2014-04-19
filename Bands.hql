-- This HQL script is meant to match a list of artist plays with a base reference list of Artist names.



-- HOW TO RUN THE SCRIPT:
-- Switch out your specifications for the Hive variables then copy and paste this line (ignore the starting "--") while connected to the HDFS. 

-- hive  --hivevar reference=base --hivevar input=sum --hivevar directory=/home/varchambault/ --hivevar filetype=txt --hivevar out_schema=varcham -f Bands.hql


-- I love using variables! So...there are five Hive variables utilized in this script:

-- 1. hivevar reference
--						This signifies the base artist list you will be importing from your HDFS. Use the file name (ex: use the word "base" for the file "base.txt").

-- 2. hivevar input
--						This signifies the plays list you will be importing from your HDFS. Use the file name (ex: use the word "sum" for the file "sum.txt").

-- 3. hivevar directory
--						This signifies the path you're calling the files from in the HDFS. Splitting the directory and file names up allows for cleaner output and better customization when running the script.

-- 4. hivevar filetype
--						I threw this in here just in case someone running the script were using another file format than txt. This script assumes the input files are tab-delimited.

-- 5. hivevar out_schema
--						This signifies the Hive schema that you will be working within. All The input tables and the output tables will all be saved within this schema. I was testing this script within my personal 'varcham' schema...so feel free to switch that out for your schema name if you want to run the script!

-- ALSO: THE FINAL PART OF THE RUN COMMAND SPECIFIES THIS SCRIPT'S FILE PATH. THIS MAY NEED TO BE CHANGED TO A FULL FILEPATH IF THE USER IS NOT RUNNING THE SCRIPT WHILE WITHIN ITS CONTAINING DIRECTORY.


-- My SUMMARY OF THE PROBLEM:
-- This script was very fun to write! The three major components of building it were:
-- - Allowing the user to bring data into hive for analysis. This involved Creating empty tables within Hive and then populating the tables with the external data from HDFS.
-- - Analyzing the two tables and joining them based on a name match. This involved finding some way to join the Base Artist list's single artist name column with the Total Plays list with the "mangled" artist name's column. 
-- - Grouping the matches to find the sum total.


-- NAME MATCH METHODOLOGY:
-- At first I ran the script with a simple RLIKE statement between the two columns. I identified that the standards RLIKE could not identify The Beatles, Foo Fighters, The Doors, and Ben Harper (and all the other incarnations of Ben Harper) properly. Since the instructions specified "Let's assume that the input examples above show all the ways that the name could be mangled..." I systematically went through these cases and customized the join using some groovy regex magic!

-- to address the issues involved misplaced "The"...I used different combinations of the phrase: 
-- CONCAT("%",${reference}.artist,"%") RLIKE CONCAT("The ",${input}.artist_play,"%")
-- This way I accounted for all possible cases for "The".

-- The only reminaing issue was the issue of "&"" replacing "And". To solve this I used the REVERSE function so that it could find a match by running through the name strings backwards. This solved the problem of Ben Harper, Ben Harper & The Innocent Criminals, and Ben Harper and the Inoocent Criminals. It distinguished Ben Harper as a solo artist with his band and also allowed both spellings of the band to find a match and participate in the sum calculation!



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

load data local inpath '${directory}${reference}.${filetype}' into table ${out_schema}.${reference};


-- Import the plays list file


!echo 'Importing Plays List'; 
drop table if exists ${out_schema}.${input};

CREATE TABLE ${out_schema}.${input}(artist_play STRING,stamp INT,plays INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

load data local inpath '${directory}${input}.${filetype}' into table ${out_schema}.${input};



-- Merge tables into new super-table!

!echo 'Joining on name match'; 
drop table if exists ${out_schema}.${reference}_compare_${input};

CREATE TABLE ${out_schema}.${reference}_compare_${input} as 
SELECT ${reference}.artist, from_unixtime(${input}.stamp, 'yyyy-MM-dd') as todays_date, sum(${input}.plays) as total_plays 
FROM ${out_schema}.${input} JOIN ${out_schema}.${reference} 
WHERE (CONCAT("%",${input}.artist_play,"%") RLIKE CONCAT("%",${reference}.artist,"%") 
	or CONCAT("%",${reference}.artist,"%") RLIKE CONCAT("The ",${input}.artist_play,"%") 
	or CONCAT("%",${input}.artist_play,"%") RLIKE CONCAT("The ",${reference}.artist,"%") 
	or CONCAT("%",${reference}.artist,",The%") RLIKE CONCAT("%The ",${input}.artist_play,"%") 
	or REVERSE(${reference}.artist) RLIKE REVERSE(${input}.artist_play)) 
GROUP BY ${reference}.artist, from_unixtime(${input}.stamp, 'yyyy-MM-dd');

-- Turn on column headers

set hive.cli.print.header=true;

-- Display results

!echo 'Here are the total number of song plays per artist:'; 

select * from ${out_schema}.${reference}_compare_${input};

!echo 'Process complete...have a great day!'