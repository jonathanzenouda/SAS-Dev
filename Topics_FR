/*****************************************************************
* SAS Visual Text Analytics
* Topics Astore Score Code
*
* Modify the following macro variables to match your needs.

* NOTE: The text variable on the input table must match the
* name and type of the text variable in the table that was used
* to create the analytic store (astore) table.
****************************************************************/
proc casutil;
droptable casdata="TOPICS_FULL_FR" incaslib="Public" quiet;
load casdata="FR_SOCMEDIA_VITAPY.sashdat" incaslib="Public" outcaslib="Public" casout="FR_SOCMEDIA_VITAPY" replace;
quit;


%let output_caslib_name="CASUSER";

%let table_name=FR_SOCMEDIA;
%let varname=socmedia_content4vta;
%let input_astore_caslib_name = "Analytics_Project_7358c1e3-3aba-4f87-95fd-66d190b6cf67";
%let input_astore_name = "23aeccaf-2348-41aa-b01e-4d75013eb2b4_TEXT_MODEL";
%let copy_vars_variable="socmedia_id","socmedia_posted_date","content";

%let output_documents_table_name = "out_documents";


proc cas;

    loadactionset "astore";

    action astore.score;
        param
            table={caslib="Public", name="FR_SOCMEDIA_VITAPY"}
            rstore={caslib=&input_astore_caslib_name, name=&input_astore_name}
            out={caslib=&output_caslib_name, name=&output_documents_table_name, replace=TRUE}
            copyVars={&copy_vars_variable};
        ;
    run;
quit;

proc sort data=CASUSER.OUT_DOCUMENTS out=OUT_DOCUMENTS;
by socmedia_id socmedia_posted_date ;
run;


proc transpose data=OUT_DOCUMENTS out=work.OUT_STACKED(drop=_Label_ 
		rename=(col1=Topic_App _Level_=_Topic_) where=(Topic_App=1)) name=_Level_;
	var _TextTopic_:;
	by  socmedia_id socmedia_posted_date ;

run;


proc transpose data=OUT_DOCUMENTS out=work.OUT_STACKED_SCORE(drop=_Label_ 
		rename=(col1=Topic_App _Level_=_Score_)) name=_Level_;
	var _Col:;
	by  socmedia_id socmedia_posted_date;
run;

data CASUSER.TOPICS_NAME;
set SASHELP.VCOLUMN(where=(memname="OUT_DOCUMENTS" and libname="CASUSER"));
if index(name,"Topic") ge 1;
keep name label;
rename name=_Topic_ label=_Topic_label;
run;


data CASUSER.OUT_STACKED ; 
set OUT_STACKED;
topic_id=input(substr(_Topic_,12,length(_Topic_)-11),8.);
drop Topic_App;
run;


data CASUSER.OUT_STACKED;
merge CASUSER.OUT_STACKED PUBLIC.FR_SOCMEDIA_VITAPY(keep=socmedia_id content);
by socmedia_id;
run;

data CASUSER.OUT_STACKED_SCORE ; 
set OUT_STACKED_SCORE;
topic_id=input(substr(_Score_,5,length(_Score_)-5),8.);
run;

proc fedsql sessref=jonathan;
create table CASUSER.OUT_FULL_STACKED{options replace=true} as 
select t1.*,t2._Score_,t2.Topic_App from CASUSER.OUT_STACKED t1 left join CASUSER.OUT_STACKED_SCORE t2
on t1.socmedia_id=t2.socmedia_id and t1.topic_id=t2.topic_id;

create table CASUSER.OUT_FULL_STACKED{options replace=true} as 
select t1.*,t2._Topic_label from CASUSER.OUT_FULL_STACKED t1 left join CASUSER.TOPICS_NAME t2
on t1._Topic_=t2._Topic_;
quit;


data CASUSER.OUT_FULL_STACKED;
merge  CASUSER.OUT_FULL_STACKED(in=in1) PG_NATO.BR_SOCMEDIA_SOCMEDIA_USER(keep=socmedia_id socmedu_id);
drop _Topic_;
if in1;
run;

/* S'assurer que la donnée USER est bien chargée en mémoire */
proc casutil;
load casdata="GEO_INFO.sashdat" incaslib="Public" outcaslib="Public" CASOUT="GEO_INFO" replace;
quit;

data Public.TOPICS_FR;
merge CASUSER.OUT_FULL_STACKED(in=in1) PG_NATO.ENT_SOCMEDIAS_USERS;
by socmedu_id;
if in1;
run;

data Public.TOPICS_FULL_FR(promote=yes);
merge Public.TOPICS_FR(in=in1) Public.GEO_INFO(keep=socmedia_id location_latitude location_longitude);
by socmedia_id;
if in1;
run;
