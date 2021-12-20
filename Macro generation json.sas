
%macro ANN_JSON_GENERATION(path,filename);



/*****************************

* Modifiable macro variables

******************************/
*%let jsonProcCodeSpec="/home/frajoz/procgenerated.sas";
*%let jsonOutputSpec="&path./ann.json/master/pool/&filename..ann.json";
%let stmtEnd=%STR(;);
filename mysas "&path./procgenerated.sas";
filename myjson "&path./Tagtog sample/&file..ann.json";

%macro fileHeader;
	put "proc json out=myjson nosastags &stmtEnd";
	put "write open object &stmtEnd ";
	put "write values 'annotatable' &stmtEnd ";
	put "write open object &stmtEnd ";
	put "write values 'parts' &stmtEnd ";
	put "write open array &stmtEnd ";

%mend fileHeader;

%macro fileLastpart;
	put "write close &stmtEnd";
	put "write close &stmtEnd";
	put "write values  'anncomplete' false &stmtEnd";
	put "write values  'sources' &stmtEnd";
	put "write open array &stmtEnd";
	put "write close &stmtEnd";
	put "write values  'metas' &stmtEnd";
	put "write open object &stmtEnd";
	put "write close &stmtEnd";
	put "write values  'entities' &stmtEnd";
	put "write open array &stmtEnd";

%mend fileLastpart;


%macro fileClose;

put "write close &stmtEnd";
put "write values 'relations' &stmtEnd ";
put "write open array &stmtEnd";
put "write close &stmtEnd";
put "write close &stmtEnd";
put "run &stmtEnd";
%mend fileClose;

data _NULL_;
set annot_id(obs=1) end=lastobs;
file "&path./procgenerated.sas";
if _N_=1 then do;
%fileHeader;
end;
x= 'write values "s1v1";';
put x;

if lastobs then do;
%fileLastpart;
do i=1 to nobs;
set CONCEPTS point=i nobs=nobs end=lastconcept;

y1='write values "classId" "'||strip(classId)||'";';
y2='write values "part" "s1v1";';
y3='write values "start" '||strip(start)||';';
y4='write values "text" "'||strip(text)||'";';

put "write open object &stmtEnd";
put y1;
put y2;
put "write values 'offsets' &stmtEnd";
put "write open array &stmtEnd ";
put "write open object &stmtEnd ";
put y3;
put y4;
put "write close &stmtEnd";
put "write close &stmtEnd";

put "write values 'coordinates' &stmtEnd " ;
put "write open array &stmtEnd ";
put "write close &stmtEnd ";

put "write values 'confidence' &stmtEnd" ;
put "write open object &stmtEnd ";
put "write values 'state' 'pre-added' &stmtEnd" ;
put "write values 'who' &stmtEnd" ;
put "write open array &stmtEnd";
put "write values 'user:Jonzen' &stmtEnd";
put "write close &stmtEnd";
put "write values 'prob' 1 &stmtEnd" ;
put "write close &stmtEnd ";

put "write values 'fields' &stmtEnd" ;
put "write open object &stmtEnd ";
put "write close &stmtEnd";

put "write values 'normalizations' &stmtEnd" ;
put "write open object &stmtEnd ";
put "write close &stmtEnd";
put "write close &stmtEnd";

end;
%fileClose;
end;

run;


%include "&path./procgenerated.sas";

%mend;
