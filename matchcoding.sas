proc casutil;
droptable casdata="DENORM_SOCMEDIA" incaslib="Public" quiet;
quit;

proc fedsql sessref=jonathan;
create table Public.HASHTAGS_AGG{options replace=true} as
select t1.socmedia_lang,t1.socmedh_name as hashtag,count(*) as Nb_posts 
from PG_NATO.DENORM_SOCMEDIAS t1
where t1.socmedh_name is not null group by t1.socmedia_lang,t1.socmedh_name;
quit;

data Public.EN_HASHTAGS_MATCHED;
set Public.HASHTAGS_AGG;
if socmedia_lang='EN' then HASHTAG_MATCHED=dqmatch (strip("HASHTAG"n), "TEXT", 90, "ENUSA");
else if socmedia_lang='FR' then HASHTAG_MATCHED=dqmatch(strip("HASHTAG"n), "TEXT", 90, "FRFRA");
else if socmedia_lang='ES' then hashtag_matched=dqmatch(strip("HASHTAG"n), "TEXT", 90, "FRFRA");
else HASHTAG_MATCHED=HASHTAG;
run;

proc fedsql sessref=jonathan;
create table CASUSER.EN_HASHTAGS_MATCHCODE{options replace=true} as 
select distinct t1.HASHTAG_MATCHED,max(t1.nb_posts) as max from Public.EN_HASHTAGS_MATCHED t1
group by t1.HASHTAG_MATCHED;

create table CASUSER.HASHTAGS_MATCHJOIN{options replace=true} as 
select distinct t1.*,t2.HASHTAG as HASHTAG_MATCHCODE from CASUSER.EN_HASHTAGS_MATCHCODE t1 
left join Public.EN_HASHTAGS_MATCHED t2 
on t1.max=t2.nb_posts and t1.HASHTAG_MATCHED=t2.HASHTAG_MATCHED ;

create table CASUSER.HASHTAG_FULLJOIN{options replace=true} as 
select distinct t1.HASHTAG_MATCHCODE,t2.HASHTAG as socmedh_name
from CASUSER.HASHTAGS_MATCHJOIN t1 left join Public.EN_HASHTAGS_MATCHED t2 
on t1.HASHTAG_MATCHED=t2.HASHTAG_MATCHED ;
quit;

data Public.DENORM_SOCMEDIA(promote=yes) ;
merge PG_NATO.DENORM_SOCMEDIAS(in=in1) CASUSER.HASHTAG_FULLJOIN;
by socmedh_name;
if in1;
if missing(socmedia_id)=0;
run;

