proc casutil;
droptable casdata="CONCEPTS_CATEGFRANCE" incaslib="Public" quiet;
droptable casdata="FACTS_CATEGRANCE" incaslib="Public" quiet;
run;

%macro boucle_langue;
%let langues=fr en;

%do i=1 %to 1;
%let langue=%CMPRES(%scan(&langues.,&i.));


%let output_caslib_name="CASUSER";
%let output_categories_table_name = "out_categories";
%let output_matches_table_name = "out_matches";
%let output_modeling_ready_table_name = "out_modeling_ready";
%let output_facts_table_name = "out_facts";
%let output_concepts_table_name = "out_concepts";

%if &i.=1 %then %do;
%let liti_binary_caslib = "Analytics_Project_7358c1e3-3aba-4f87-95fd-66d190b6cf67";
%let mco_binary_caslib = "Analytics_Project_7358c1e3-3aba-4f87-95fd-66d190b6cf67";
%let mco_binary_table_name = "2c9d2fc779d0f2a4017a0ff056ee000c_CATEGORY_BINARY";
%let liti_binary_table_name = "2c9d2fc779a2a1900179c6cdfb7f0132_CONCEPT_BINARY";
%let table_name=FR_SOCMEDIA;
%let varname=socmedia_content4vta;
%let dqlang="FRFRA";
%let language="FRENCH";
%end;
%else %do;
%let mco_binary_caslib = "Analytics_Project_c0ddf0f4-57e1-4642-98a2-64a1f438f19f";
%let mco_binary_table_name = "2c9d2fc779a2a15a0179c2de33a80005_CATEGORY_BINARY";
%let liti_binary_caslib = "Analytics_Project_c0ddf0f4-57e1-4642-98a2-64a1f438f19f";
%let liti_binary_table_name = "2c9d2fc779a2a1900179a3a979f60000_CONCEPT_BINARY";
%let table_name=EN_SOCMEDIA_CLEANED;
%let dqlang="ENUSA";
%let varname=socmedia_cleaned_text;
%let language="ENGLISH";
%end;


proc casutil;
load casdata="&table_name..sashdat" incaslib="Public" outcaslib="Public" replace casout="&table_name.";
quit;
/* calls the scoring action */
proc cas;

    loadactionset "textRuleScore";

    action applyCategory;
        param
            model={caslib=&mco_binary_caslib, name=&mco_binary_table_name}
            table={caslib="PUBLIC", name="&table_name."}
            docId="socmedia_id"
            text="&varname."
            casOut={caslib=&output_caslib_name, name=&output_categories_table_name, replace=TRUE}
            matchOut={caslib=&output_caslib_name, name=&output_matches_table_name, replace=TRUE}
            modelOut={caslib=&output_caslib_name, name=&output_modeling_ready_table_name, replace=TRUE}
        ;
    run;
quit;

data casuser.&langue._CATEG;
merge PUBLIC.&table_name. CASUSER.OUT_MODELING_READY(where=(category_1=1) in=in1);
by socmedia_id;
if in1;
run;

/******************************EXTRACTION ***************************/
/* calls the scoring action */
proc cas;

    loadactionset "textRuleScore";

    action applyConcept;
        param
            model={caslib=&liti_binary_caslib, name=&liti_binary_table_name}
            table={caslib="CASUSER", name="&langue._CATEG"}
            docId="socmedia_id"
            text="&varname."
            casOut={caslib=&output_caslib_name, name=&output_concepts_table_name, replace=TRUE}
            factOut={caslib=&output_caslib_name, name=&output_facts_table_name, replace=TRUE}
        ;
    run;
quit;

proc sort data=CASUSER.OUT_CONCEPTS(drop=_canonical_form_) out=CONCEPTS;
by socmedia_id _concept_ _start_;
run;

data CONCEPTS_CALCUL;
set CONCEPTS;

by socmedia_id _concept_;
prevstart=lag(_start_);
prevend=lag(_end_);
L_MATCH=length(_match_text_);
retain result_id 1;
if (_start_>prevend or _start_<prevstart) then do;
result_id+1;
end;
run;

proc sql;
create table Public.CONCEPTS_DEDUP as 
select socmedia_id,_concept_,_start_,_end_,_match_text_,result_id
from CONCEPTS_CALCUL group by socmedia_id,_concept_,result_id 
having L_MATCH=max(L_MATCH) ;
quit;
/*
data Public.&langue._CONCEPTS_CATEGFRANCE;
merge CASUSER.&langue._CATEG PUBLIC.CONCEPTS_DEDUP;
by socmedia_id;
run;*/


proc fedsql sessref=jonathan;
create table Public.CONCEPT_AGG{options replace=true} as
select t1._concept_,t1._match_text_,count(*) as Nb_posts 
from PUBLIC.CONCEPTS_DEDUP t1
where t1._match_text_ is not null group by t1._concept_,t1._match_text_;
quit;


data Public.CONCEPT_AGG;
set Public.CONCEPT_AGG;
matchcode=dqmatch(_match_text_, "TEXT", 90, &dqlang.);
run;

proc sql ;
create table CONCEPT_MAXCODE as 
select _match_text_ as matchcode_text,matchcode from Public.CONCEPT_AGG group by matchcode having NB_POSTS=max(NB_POSTS);

create table CONCEPT_JOIN as 
select t1.*,t2._concept_,t2._match_text_,t2.NB_POSTS 
from CONCEPT_MAXCODE t1 left join PUBLIC.CONCEPT_AGG t2 on t1.matchcode=t2.matchcode;

create table CONCEPT_FULLJOIN as 
select t1.*,t2.matchcode_text 
from PUBLIC.CONCEPTS_DEDUP t1 left join CONCEPT_JOIN t2 
on t1._match_text_=t2._match_text_ ;
quit;

proc copy in=WORK out=CASUSER;
select CONCEPT_FULLJOIN;
run;

data Public.&langue._CONCEPTS_CATEGFRANCE;
merge CASUSER.&langue._CATEG CASUSER.CONCEPT_FULLJOIN;
by socmedia_id;
run;

************** FACTS ****************;
proc casutil;
droptable casdata="FACTS_&langue.CATEG" incaslib="Public" quiet;
quit;



proc sort data=CASUSER.OUT_FACTS out=SORTFACTS;
by socmedia_id _start_ _end_;
run;

data FACTS_CALCUL;
set SORTFACTS(where=(_fact_argument_=" "));
by socmedia_id _start_;
prevstart=lag(_start_);
prevend=lag(_end_);
retain result_id 1;
if (_start_<prevstart or _start_ > prevend) then do;
result_id+1;
end;
run;

proc sql;
create table FACTS_CALCULDEDUP as 
select socmedia_id,_fact_,_result_id_,result_id,_start_,_end_
from FACTS_CALCUL 
group by socmedia_id,_fact_,result_id 
having length(_match_text_)=max(length(_match_text_));

create table Public.FACTS_DEDUP as 
select t1.socmedia_id,t1.result_id,t1._fact_,
case when t2._fact_argument_=" " then "full_text" 
else t2._fact_argument_
end as _fact_argument_,t2._match_text_,t1._start_,t1._end_ 
from FACTS_CALCULDEDUP t1 left join SORTFACTS t2 
on t1.socmedia_id=t2.socmedia_id and t1._fact_=t2._fact_ and t1._result_id_=t2._result_id_;
quit;

data Public.FACTS_DEDUPAGG;
set Public.FACTS_DEDUP;
if _fact_argument_="name" then matchcode=dqmatch(_match_text_, "NAME", 85, &dqlang.);
if _fact_argument_="title" then matchcode=dqmatch(lowcase(_match_text_), "TEXT", 85, &dqlang.);
else matchcode=dqmatch(_match_text_,"TEXT",85,&dqlang.);
run;

proc sql;
create table FACTS_DEDUPAGG as 
select t1._fact_,t1._fact_argument_,t1.matchcode,t1._match_text_ as matchcode_text
from Public.FACTS_DEDUPAGG t1 group by _fact_,_fact_argument_,matchcode
having length(_match_text_)=max(length(_match_text_)) ;

create table FACTS_DEDUPJOIN as 
select distinct t1.*,case when t1._fact_argument_="title" then lowcase(t2.matchcode_text )
else t2.matchcode_text 
end as matchcode_text
from Public.FACTS_DEDUPAGG t1 left join FACTS_DEDUPAGG t2 
on t1.matchcode=t2.matchcode;
run;

proc copy in=WORK out=Public;
select FACTS_DEDUPJOIN;
run;

proc cas;
transpose.transpose /                                         
   table={name="FACTS_DEDUPJOIN", caslib="Public",groupby={"socmedia_id","result_id","_fact_"} }                     /*1*/
   attributes={"socmedia_id","result_id","_fact_"}               /*2*/
   transpose={"matchcode_text"}                                  /*3*/
   id={"_fact_argument_"}                                         /*4*/
   casOut={name="FACTS_TRSP", replace=true} 
let=TRUE ;                   /*5*/
run;


data Public.FACTS_&langue.CATEG;
merge CASUSER.&langue._CATEG CASUSER.FACTS_TRSP;
by socmedia_id;
drop _NAME_ _LABEL_;
run;





/****************************TOPIC DETECTION *******************/
proc cas;                                            
   loadtable caslib="ReferenceData" path="&langue._stoplist.sashdat" casOut={caslib="ReferenceData",name="&langue._stoplist",replace=True}; 
quit;


proc cas;                                             /* 4*/

   loadactionset "textMining";                  
   action tmMine;
   param
   docId="socmedia_docid"
   documents={ name="&langue._CATEG",caslib="CASUSER"}
   text="&varname."
   nounGroups= True
   tagging = True
   stemming= True
   stopList ={ name="&langue._stoplist",caslib="ReferenceData"}
   parseConfig={name="config", replace=TRUE}
   parent ={ name="parent",replace=TRUE}
   offset ={name="offset",replace=TRUE}
   terms ={ name="terms", replace=TRUE}
   reduce=10
   topics ={ name="topicsSVD", replace=TRUE}
   topicDecision=True
	k=15
   docPro ={ name="docpro", replace=TRUE}
   u ={ name="svdu", replace=TRUE}
language=&language.

   ;
   run;
quit;


data CASUSER.STARTLIST;
set CASUSER.TERMS;
if _Role_ not in ("PUNC","nlpNounGroup","NUM","INTJ","DET","PRO","ADV");

keep _Term_ _Termnum_ _Role_;
rename _Term_=Term _Termnum_=_Index_ _Role_=ROLE;
run;

proc cas;                                             /* 4*/

   loadactionset "textMining";                  
   action tmMine;
   param
    docId="socmedia_docid"
   documents={ name="&langue._CATEG",caslib="CASUSER"}
   text="&varname."
   nounGroups= True
   tagging = True
   stemming= True
   startList ={ name="STARTLIST"}
   parseConfig={name="config", replace=TRUE}
   parent ={ name="parent",replace=TRUE}
   offset ={name="offset",replace=TRUE}
   terms ={ name="terms", replace=TRUE}
   topics ={ name="topicsSVD", replace=TRUE}
   reduce=2
   docPro ={ name="docpro", replace=TRUE}
   u ={ name="svdu", replace=TRUE}
	k=30
   topicDecision=True
language=&language.
   ;
   run;

quit;

proc cas;
   action table.fetch /table="topicsSVD", orderBy="_TopicID_" to=100; 
   run;
quit;
%end;
%mend;
%boucle_langue;

data Public.CONCEPTS_CATEGRANCE(promote=yes);
set Public.FR_CONCEPTS_CATEGFRANCE;
run;

data Public.FACTS_CATEGRANCE(promote=yes);
set Public.FACTS_FRCATEG;
run;
