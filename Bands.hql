-- AUTHOR: Vanessa Archambault
-- CONTACT: varch02@gmail.com
-- DATE: 4/19/2014
-- TIME TAKEN: In total...about three hours... although it's hard to tell because I was on vacation and wrote it in multiple small sprints :)
-- PURPOSE: Beats code challenge!!!

-- DESCRIPTION: This HQL script is meant to match a list of artist plays within a certain day with a base reference list of Artist names. It then sums up the total plays per artist and displays: Artist name, date of the plays, total number of plays.



-- HOW TO RUN THE SCRIPT:
-- Switch out your specifications for the Hive variables. Then copy and paste the following line (ignore the starting "--") while connected to the HDFS: 

-- hive  --hivevar reference=base --hivevar input=sum --hivevar directory=/home/varchambault/ --hivevar filetype=txt --hivevar out_schema=varcham -f Bands.hql


-- VARIABLE EXPLANATION:

-- I love using variables! So...there are five Hive variables utilized in this script:

-- 1. hivevar reference
--						This signifies the base artist list you will be importing from your HDFS. Use the file name (ex: use the word "base" for the file "base.txt").

-- 2. hivevar input
--						This signifies the artist plays within a certain day list you will be importing from your HDFS. Use the file name (ex: use the word "sum" for the file "sum.txt").

-- 3. hivevar directory
--						This signifies the path you're calling the files from in the HDFS. Splitting the directory and file names up allows for cleaner output and better customization when running the script.

-- 4. hivevar filetype
--						I threw this in here just in case someone running the script were using another file format than txt. This script assumes the input files are tab-delimited.

-- 5. hivevar out_schema
--						This signifies the Hive schema that you will be working within. All The input tables and the output tables will all be saved within this schema. I was testing this script within my personal 'varcham' schema...so feel free to switch that out for your schema name if you want to run the script!

-- ALSO: THE FINAL PART OF THE RUN COMMAND SPECIFIES THIS SCRIPT'S FILE PATH. THIS MAY NEED TO BE CHANGED TO A FULL FILEPATH IF THE USER IS NOT RUNNING THE SCRIPT WHILE WITHIN ITS CONTAINING DIRECTORY.


-- My SUMMARY OF THE PROBLEM:
-- This script was very fun to write! The three major components of building it were:
-- 1. Allowing the user to bring data into hive for analysis. This involved Creating empty tables within Hive and then populating the tables with the external data from HDFS.
-- 2. Analyzing the two tables and joining them based on a name match. This involved finding some way to join the Base Artist list's single artist name column with the Total Plays list with the "mangled" artist name's column. 
-- 3. Grouping the matches to find the sum total.


-- NAME MATCH METHODOLOGY:
-- At first I ran the script with a simple RLIKE statement between the two columns. I identified that the standard RLIKE could not identify The Beatles, Foo Fighters, The Doors, and Ben Harper (and all the other incarnations of Ben Harper) properly. Since the instructions specified "Let's assume that the input examples above show all the ways that the name could be mangled..." I systematically went through these cases and customized the join using some groovy regex magic!

-- To address the issues involving oddly placed word "The"...I used different combinations of the phrase: 
-- CONCAT("%",${reference}.artist,"%") RLIKE CONCAT("The ",${input}.artist_play,"%")
-- This way I accounted for all possible cases for "The". By experimenting with concatenating "The" before and after both the base artist and the total plays artist names it allowed the tables to find the proper names to match and join on.

-- The only reminaing issue was the issue of "&"" replacing "And". To solve this I used the REVERSE function so that it could find a match by running through the name strings backwards. This solved the problem of Ben Harper, Ben Harper & The Innocent Criminals, and Ben Harper and the Inoocent Criminals. It distinguished Ben Harper as a solo artist with his band and also allowed both spellings of the band to find a match and participate in the sum calculation!

-- ODD CASES:
-- Ke$ha changed her name to Kesha. That would be very difficult to match up in a join! Although if we knew beforehanf that we would need to account for that we could use a REGEX_REPLACE in the artist plays table before the merge to replace all instances of Ke$ha with Kesha (or vice-versa) to ensure these special cases of odd spelling are dealt with before the join attempt.
-- If artists with compound names such as "The Wallflowers" were listed as "The Wall Flowers" the script may take issue.
-- Artists that go by acronyms such as "AFI" versus "A Fire Inside" could not be matched.

-- OTHER ASPECTS:
-- After successfully getting the tables to join onto eachother it only took grouping by the artist and the date to sucessfully run the SUM function. This gave the user the total numbers of plays per artist. 

-- Converting the Unix timestamp from an integer to a date form was simple with the FROM_UNIXTIME function.

-- Personally, I prefer the DROP TABLE IF EXISTS/CREATE replacement table method for Hive querying. It allows the user to refer back to the output table after the fact. It makes for a clean table/schema structure since the naming structure of the tables created within the schema can be controlled by the user's hivevar specifications.

-- I did not need to write a UDF for this script. All functions that were needed are included within the Hive base package.

-- I decided to display the results at the end of the script although this should be removed if the script is used to query larger sets of data/artists. I set the headers to "true" so the display would be neat and pretty. I enjoy a friendly 'echo' message, don't you? :)




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
SELECT ${reference}.artist, FROM_UNIXTIME(${input}.stamp, 'yyyy-MM-dd') as todays_date, SUM(${input}.plays) as total_plays 
FROM ${out_schema}.${input} JOIN ${out_schema}.${reference} 
WHERE ((${input}.artist_play) = (${reference}.artist) 
	or (${reference}.artist) RLIKE CONCAT("The ",${input}.artist_play) 
	or CONCAT("%",${input}.artist_play,"%") RLIKE CONCAT("The ",${reference}.artist,"%") 
	or CONCAT(${reference}.artist,",The%") RLIKE CONCAT("%The ",${input}.artist_play,"%") 
	or REVERSE(${reference}.artist) RLIKE REVERSE(${input}.artist_play)
	or (${reference}.artist) rlike regexp_replace(${input}.artist_play, 'and the', '& The')) 
GROUP BY ${reference}.artist, FROM_UNIXTIME(${input}.stamp, 'yyyy-MM-dd');

-- Turn on column headers

set hive.cli.print.header=true;

-- Display results

!echo 'Here are the total number of song plays per artist:'; 

select * from ${out_schema}.${reference}_compare_${input};

!echo 'Output schemas: ${out_schema}.${reference}_compare_${input}'
!echo 'Process complete...have a great day!'