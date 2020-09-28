/******************************************************************************/
/* Program:       get_targetoracledb_table.SAS                            				 */
/*                                                                           	 */
/* Description:   Creates SAS datasets in targetoracledb loca user       					 */


%let work_path=%sysfunc(pathname(work));
/*%MACRO UPLOAD_SAS_DATASET(outsch,outdsn, indsn, inlib,datevar , dategreaterthan ,datelessthan,extracondition);*/
%MACRO UPLOAD_SAS_DATASET(outsch,outdsn, indsn, inlib,datevar , dategreaterthan ,datelessthan);
   
   %let datetime_start =%sysfunc(datetime(),datetime20.2);

	 %if %length(&datevar.)> 0 and  %length(&dategreaterthan.)> 0 %then %do; 
       %let condition1= and DATEPART(&datevar.) >= INPUT(&dategreaterthan., DDMMYY10.) ;
          %end;
          	%else 
	  	 	%do;
			%let condition1= and 1=1;
			%end;		
	 %if %length(&datevar.)> 0 and  %length(&dategreaterthan.)> 0 %then %do; 
       %let condition2=and DATEPART(&datevar.) <= INPUT(&datelessthan., DDMMYY10.) ;
          %end;
          	%else 
	  	 	%do;
			%let condition2= and 1=1;
			%end;		

/*	%if %length(&extracondition.) > 0 %then %do; */
/*       %let condition3= and &extracondition. ;*/
/*          %end;*/
/*          	%else */
/*	  	 	%do;*/
/*			%let condition3= and 1=1;*/
/*			%end;*/

	 	LIBNAME INLIB1  &inlib. ;
/*		%let inlib=&inlib.;*/
    proc sql NOERRORSTOP;
  	 drop table &outsch..&outdsn.;
        create table &outsch..&outdsn. as
/*           select * From &inlib..&indsn.*/
		select * From INLIB1.&indsn.
		   where 1=1  &condition1.  &condition2.   
	    ;
   quit;

	%let dsid=%sysfunc(open(&outsch..&outdsn.));
   %let nobs=%sysfunc(attrn(&dsid.,NOBS));
   %let dsid=%sysfunc(close(&dsid.));

   
   %let datetime_end =%sysfunc(datetime(),datetime18);
   %let dur = %sysfunc(putn(%sysevalf("&datetime_end."dt - "&datetime_start."dt), time10.2));

   %put **************************************;
   %put EXTRACT STATS;
   %put **************************************;
   %put START TIME:�       &datetime_start.;
   %put END TIME:�         &datetime_end.;
   %put PROCESSING TIME: � &dur.;
   %put DATASET CREATED:   &outlib..&outdsn.;
   %put RECORD COUNT:      &nobs.;
   %put **************************************;

  
%MEND UPLOAD_SAS_DATASET;

%let datetime_start_prog =%sysfunc(datetime(),datetime20.2);

PROC SQL;
DROP TABLE AR_CONTROL_TABLE;
CREATE TABLE AR_CONTROLTABLE AS SELECT distinct 
    sourceoracledbschema,
	cat('"',strip(saslib),'"') as saslib,
    sourceoracledbtablename,
    sasdatasetname,
    datavar,
    dategreaterthan,
    datelessthan,
    datenotin,
/*    extracondition,*/
    excludcolumnlist,
    targetoracledbrecon_ind,
    sasrecon_ind,
    comments 
FROM
    home.ar_controltable where sasrecon_ind='Y';

DATA _null_;
SET AR_CONTROLTABLE end=lastobs;
call symput ('saslib'||put(_N_,3.-L),saslib);
call symput ('sasdatasetname'||put(_N_,3.-L),sasdatasetname);
call symput ('datevar'||put(_N_,3.-L),datavar);
call symput ('dategreaterthan'||put(_N_,3.-L),dategreaterthan);
call symput ('datelessthan'||put(_N_,3.-L),datelessthan);
/*call symput ('extracondition'||put(_N_,3.-L),extracondition);*/
if lastobs then 
call symput ('saslib',put(_N_,3.-L));
call symput ('num_sasdatasetnames',put(_N_,3.-L));
call symput ('datevar',put(_N_,3.-L));
call symput ('dategreaterthan',put(_N_,3.-L));
call symput ('datelessthan',put(_N_,3.-L));
/*call symput ('extracondition',put(_N_,3.-L));*/
run;


%macro loop ;
%do i=1 %to &num_sasdatasetnames;
/*LIBNAME INLIB &&saslib&i;*/
	%UPLOAD_SAS_DATASET(
				OUTSCH=OUTSCH, 
	            outdsn=&&sasdatasetname&i, 
	            indsn=&&sasdatasetname&i,
				inlib=&&saslib&i, 
				datevar=&&datevar&i,
				dategreaterthan=&&dategreaterthan&i, 
				datelessthan=&&datelessthan&i
/*				extracondition=&&extracondition&i*/
	           );
%end;
%mend loop;

%loop;

%let datetime_end_prog =%sysfunc(datetime(),datetime20.2);
%let dur_prog = %sysfunc(putn(%sysevalf("&datetime_end_prog."dt - "&datetime_start_prog."dt), time10.2));
%put **************************************;
%put PROGRAM STATS;
%put **************************************;
%put START TIME:�       &datetime_start_prog.;
%put END TIME:�         &datetime_end_prog.;
%put PROCESSING TIME: � &dur_prog.;
%put **************************************;
