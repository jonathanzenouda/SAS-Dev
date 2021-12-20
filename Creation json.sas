/*FIRST EXECUTE "Generation proc Json" then the code which is in comment has to be "uncomment" in order to refresh the data*/

/******************************REFRESH DATA ******************************/
proc datasets memtype=data lib=work kill;run;

data public.VO_txt_SAMP;
    set public.vo_txt_new (obs=50);
    ID= _n_;
run;

%let liti_binary_caslib = "Analytics_Project_c0064886-a7af-445b-bc8c-345f3323084f";

%let liti_binary_table_name = "8aaa8126790f0dc80179220e520d001e_CONCEPT_BINARY";

proc casutil ;
droptable casdata="OUTCONCEPT" incaslib="CASUSER" quiet;
quit;


proc cas;

    loadactionset "textRuleScore";

    action applyConcept;
        param
            model={caslib=&liti_binary_caslib., name=&liti_binary_table_name.}
            table={caslib="Public", name="VO_txt_SAMP"}
            docId="ID"
            text="content"
            casOut={caslib="CASUSER", name="outconcept", replace=TRUE}
        ;
    run;
quit;

data Public.VO_TXT_CONCEPTS ;
set CASUSER.OUTCONCEPT;
run;

%let path=/srv/nfs/kubedata/compute-landingzone/Annotation project;

 
filename annot "&path./annotations-legend.json";

data CASUSER.VO_FULL_TXT;
merge Public.VO_TXT_SAMP(keep=ID filename) Public.VO_TXT_CONCEPTS;
by ID;
_start_2=_start_-1;
drop _start_;
rename _start_2=_start_;
run;

proc sort data=CASUSER.VO_FULL_TXT out=VO_FULL_TXT;
by ID;
run;

data VO_FULL_TXT;
set VO_FULL_TXT;
by ID;
if first.ID then result_id=1;
else do;
result_id+1;
end;
run;


proc sql;
create table CONCEPTS_NOM as 
select distinct _concept_ from VO_FULL_TXT where _concept_ is not null;

create table CONCEPTS_NOM as 
select distinct _concept_,"e_"||strip(put(monotonic(),8.)) as classId from CONCEPTS_NOM;

create table CONCEPTS_TABLE as 
select t1.filename,t2.classId,"s1p"||strip(put(t1.result_id,8.)) as part, t1._start_ as start, t1._match_text_ as text
from VO_FULL_TXT t1 left join CONCEPTS_NOM t2 on t1._concept_=t2._concept_ having t1._start_ is not null;
quit;

data _null_;
file annot;
set CONCEPTS_NOM end=lastobs;
if _N_=1 then do;
put "{";
end;
x='"'||strip(classID)||'":"'||strip(_concept_)||'",';
if lastobs then do;
x='"'||strip(classID)||'":"'||strip(_concept_)||'"}';
end;
put x;
run;



%macro looptagtog;
proc sql ;
select distinct filename into :listfile separated by "¤" from CONCEPTS_TABLE where filename is not null;
quit;

%do i=1 %to %sysfunc(countw(&listfile.,"¤"));
%let file=%scan(&listfile.,&i.,"¤");
 %put &file.;
filename myhtml "&path./Tagtog sample/&file..txt";

data _null_;
set Public.VO_TXT_SAMP(where=(filename="&file."));
*file "&path.plain.html/pool/&file..html" ;
file myhtml ;
put content;
run;


data ANNOT_ID;
set VO_FULL_TXT(where=(filename="&file.") keep=filename result_id);
part="s1p"||strip(result_id);
drop filename result_id;
run;

proc delete data=CONCEPTS;run;


data CONCEPTS;
set CONCEPTS_TABLE(where=(filename="&file."));
run;


%ANN_JSON_GENERATION(&path.,&file.)

%end;
%mend;
%looptagtog;
