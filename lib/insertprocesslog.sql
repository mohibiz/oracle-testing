 
--To Do's 
--Add another colum for inserting errors if any

--inserting process logs for execution

prompt "inserting process logs for execution"
	define exestructurecrecon='''&&1''';	
	define exerowcountcrecon='''&&2''';  
	define exeallcolumncrecon='''&&3'''; 
    define sourceoracledbnetservicename='''&&4'''; 
    define targetoracledbnetservicename='''&&5'''; 
    define cdbnetservicename='''&&6'''; 
    define targetoracledbusername='''&&7'''; 

DEFINE logpath=&&8;
DEFINE logfilename=&&9;
DEFINE recontarget=&&10; 
SPOOL &&logpath/&&logfilename

SET echo on;
SET SERVEROUTPUT ON ;
SET timing on;
	
     
DECLARE
  VAREXECUTION_SK NUMBER;
  INVALID_PROCEDURE EXCEPTION;
    --pragma exception_init( INVALID_PROCEDURE, -06550 );
BEGIN
    SELECT EXECUTION_ID INTO VAREXECUTION_SK FROM AR_EXECUTION_CONTROLER;
	MERGE INTO AR_EXECUTION_REQUEST olddata
	USING 
	(
	SELECT VAREXECUTION_SK  execution_sk,&&exestructurecrecon exestructurecrecon,&&exerowcountcrecon exerowcountcrecon,&&exeallcolumncrecon exeallcolumncrecon,&&sourceoracledbnetservicename sourceoracledbnetservicename,&&targetoracledbnetservicename targetoracledbnetservicename, &&cdbnetservicename cdbnetservicename, SYSDATE created_dte, &&targetoracledbusername created_by, SYSDATE request_submit_dte,SYSDATE request_end_dte,'RUNNING' request_status, 'No Comments'comments
    FROM DUAL
	) newdata
	ON 
	(
	olddata.EXECUTION_SK=newdata.EXECUTION_SK
	)
	WHEN MATCHED THEN UPDATE  SET request_status='COMPLETED' , request_end_dte=SYSDATE WHERE EXECUTION_SK=VAREXECUTION_SK
	WHEN NOT MATCHED THEN
	     INSERT (execution_sk,exestructurecrecon,exerowcountcrecon,exeallcolumncrecon,sourceoracledbnetservicename,targetoracledbnetservicename, cdbnetservicename, created_dte,created_by,request_submit_dte,request_end_dte,request_status,comments)
     VALUES(newdata.execution_sk,newdata.exestructurecrecon,newdata.exerowcountcrecon,newdata.exeallcolumncrecon,newdata.sourceoracledbnetservicename,newdata.targetoracledbnetservicename, newdata.cdbnetservicename, newdata.created_dte,newdata.created_by,newdata.request_submit_dte,newdata.request_end_dte,newdata.request_status,newdata.comments);
       
    COMMIT;
   
	EXCEPTION
		WHEN others THEN 

        MERGE INTO AR_EXECUTION_REQUEST olddata
	USING 
	(
	SELECT VAREXECUTION_SK  execution_sk,&&exestructurecrecon exestructurecrecon,&&exerowcountcrecon exerowcountcrecon,&&exeallcolumncrecon exeallcolumncrecon,&&sourceoracledbnetservicename sourceoracledbnetservicename,&&targetoracledbnetservicename targetoracledbnetservicename, &&cdbnetservicename cdbnetservicename, SYSDATE created_dte, &&targetoracledbusername created_by, SYSDATE request_submit_dte,SYSDATE request_end_dte,'FAILED' request_status,'No Comments' comments
    FROM DUAL
	) newdata
	ON 
	(
	olddata.EXECUTION_SK=newdata.EXECUTION_SK
	)
	WHEN MATCHED THEN UPDATE  SET request_status='COMPLETED' , request_end_dte=SYSDATE ,COMMENTS='FAILED TO FINISH WITHOUT ERRORS' WHERE EXECUTION_SK=VAREXECUTION_SK
	WHEN NOT MATCHED THEN
	     INSERT (execution_sk,exestructurecrecon,exerowcountcrecon,exeallcolumncrecon,sourceoracledbnetservicename,targetoracledbnetservicename, cdbnetservicename, created_dte,created_by,request_submit_dte,request_end_dte,request_status,comments)
     VALUES(newdata.execution_sk,newdata.exestructurecrecon,newdata.exerowcountcrecon,newdata.exeallcolumncrecon,newdata.sourceoracledbnetservicename,newdata.targetoracledbnetservicename, newdata.cdbnetservicename, newdata.created_dte,newdata.created_by,newdata.request_submit_dte,newdata.request_end_dte,newdata.request_status,newdata.comments);
       
    COMMIT;
        



END;
/

EXIT