/******************************************************************************/
/* Program:       get_targetoracledb_table.SAS                             			 */
/*                                                                            */
/* Description:   Creates SAS datasets in targetoracledb loca user 						  */



/******************************************************************************/

%let work_path=%sysfunc(pathname(work));

libname OUTLIB "%sysfunc(pathname(home))";

%MACRO GET_targetoracledb_TABLE(schema, indsn, outlib, outdsn);
   
   %let datetime_start =%sysfunc(datetime(),datetime20.2);

   proc sql;
      connect to targetoracledb(&conn.);
	  drop table &outlib..&outdsn.;
      create table &outlib..&outdsn. as
      select * from connection to targetoracledb
         (select * from &schema..&indsn.
         );
        disconnect from targetoracledb;
   quit;

	%let dsid=%sysfunc(open(&outlib..&outdsn.));
   %let nobs=%sysfunc(attrn(&dsid.,NOBS));
   %let dsid=%sysfunc(close(&dsid.));

   
   %let datetime_end =%sysfunc(datetime(),datetime18);
   %let dur = %sysfunc(putn(%sysevalf("&datetime_end."dt - "&datetime_start."dt), time10.2));

   %put **************************************;
   %put EXTRACT STATS;
   %put **************************************;
   %put START TIME:        &datetime_start.;
   %put END TIME:          &datetime_end.;
   %put PROCESSING TIME:   &dur.;
   %put DATASET CREATED:   &outlib..&outdsn.;
   %put RECORD COUNT:      &nobs.;
   %put **************************************;

%MEND GET_targetoracledb_TABLE;

%let datetime_start_prog =%sysfunc(datetime(),datetime20.2);


%GET_targetoracledb_TABLE(schema=&schema., 
            indsn=AR_CONTROLTABLE, 
            outlib=OUTLIB, 
            outdsn=AR_CONTROLTABLE
           );




%let datetime_end_prog =%sysfunc(datetime(),datetime20.2);
%let dur_prog = %sysfunc(putn(%sysevalf("&datetime_end_prog."dt - "&datetime_start_prog."dt), time10.2));
%put **************************************;
%put PROGRAM STATS;
%put **************************************;
%put START TIME:        &datetime_start_prog.;
%put END TIME:          &datetime_end_prog.;
%put PROCESSING TIME:   &dur_prog.;
%put **************************************;
