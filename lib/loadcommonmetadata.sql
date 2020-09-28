
prompt "generating metada for recon based on control file "
	define exestructurecrecon='''&&1''';	
	define exerowcountcrecon='''&&2''';  
	define exeallcolumncrecon='''&&3'''; 
    define executetargetoracledbrecon='''&&4'''; 
    define executesasrecon='''&&5'''; 
	DEFINE logpath=&&6;
	DEFINE logfilename=&&7;
	
	SPOOL &&logpath/&&logfilename
	
	SET echo on;
	SET SERVEROUTPUT ON ;
	SET timing on;
    --create another variable to form and pass the execture condition to control table
--EXEC proc_generatemetdaforrecon(&&exestructurecrecon ,&&exerowcountcrecon, &&exeallcolumncrecon );

DECLARE
    LOG_SK NUMBER;
    PARAM_LOG_SK NUMBER;
    EXECUTION_ID NUMBER DEFAULT -1;
    VAR_EXESTRUCTURECRECON VARCHAR(2);
    VAR_EXEROWCOUNTCRECON VARCHAR(2);
    VAR_EXEALLCOLUMNCRECON VARCHAR(2);
    VAR_EXECUTEtargetoracledbRECON VARCHAR(2);
    VAR_EXECUTEsasRECON VARCHAR(2);
    VAR_validity number;
    pragma autonomous_transaction;
   VAR_RECONTARGET VARCHAR2(30) DEFAULT 'NOT DEFINED';
BEGIN
    LOG_SK:=SEQ_EXECUTIONLOG.NEXTVAL;
    PARAM_LOG_SK:=LOG_SK;

    SELECT EXECUTION_ID INTO EXECUTION_ID FROM AR_EXECUTION_CONTROLER; --handle exception for values not found
    
    INSERT INTO AR_EXECUTION_LOG (LOG_SK,EXECUTION_BK,PROCEDURE_NME,EXECUTION_STATUS,EXECUTION_ERROR,RECON_START_DTE, RECON_END_DTE, COMMENTS)
    VALUES(PARAM_LOG_SK ,EXECUTION_ID,'PROC_LOADCOMMONMETADATA','RUNNING',' ',SYSDATE, SYSDATE, VAR_RECONTARGET);
    COMMIT;
	VAR_EXESTRUCTURECRECON:=&&exestructurecrecon;
    VAR_EXEROWCOUNTCRECON:=&&exerowcountcrecon;
    VAR_EXEALLCOLUMNCRECON:=&&exeallcolumncrecon;
	VAR_EXECUTEtargetoracledbRECON:=&&executetargetoracledbrecon;
    VAR_EXECUTEsasRECON:=&&executesasrecon;  
        	IF (VAR_EXECUTEtargetoracledbRECON ='Y' AND VAR_EXECUTEsasRECON ='N' ) THEN VAR_RECONTARGET:= 'sourceoracledb-targetoracledb';
	ELSIF (VAR_EXECUTEtargetoracledbRECON ='N' AND VAR_EXECUTEsasRECON ='Y ' )  THEN VAR_RECONTARGET:= 'sourceoracledb-sas';
    ELSIF (VAR_EXECUTEtargetoracledbRECON ='Y' AND VAR_EXECUTEsasRECON ='Y ' )  THEN VAR_RECONTARGET:= 'sourceoracledb-targetoracledb And sourceoracledb-sas';
	ELSE  VAR_RECONTARGET:= 'NA';
	END IF;
    PROC_LOADCOMMONMETADATA(VAR_EXESTRUCTURECRECON ,VAR_EXEROWCOUNTCRECON, VAR_EXEALLCOLUMNCRECON,VAR_EXECUTEtargetoracledbRECON,VAR_EXECUTEsasRECON,PARAM_LOG_SK);
  --  VAR_validity:=check_object_validity( 'GUPTAMOH','PROCEDURE' , 'PROC_LOADCOMMONMETADATA');
    
 /* IF   VAR_validity =1
    THEN
	PROC_LOADCOMMONMETADATA(VAR_EXESTRUCTURECRECON ,VAR_EXEROWCOUNTCRECON, VAR_EXEALLCOLUMNCRECON,VAR_EXECUTEtargetoracledbRECON,VAR_EXECUTEsasRECON,PARAM_LOG_SK);
  ELSE
  UPDATE  AR_EXECUTION_LOG SET EXECUTION_STATUS='COMPLETED WITH ERROR' 
            , RECON_END_DTE=SYSDATE ,EXECUTION_ERROR ='Invalid Procedure'
            ,COMMENTS='Procedure is in invalid state'
            WHERE LOG_SK=PARAM_LOG_SK;
  END IF ;*/
    UPDATE  AR_EXECUTION_LOG SET EXECUTION_STATUS='COMPLETED' , RECON_END_DTE=SYSDATE WHERE LOG_SK=PARAM_LOG_SK;
    COMMIT;
    
    EXCEPTION
    WHEN others THEN
            UPDATE  AR_EXECUTION_LOG SET EXECUTION_STATUS='COMPLETED WITH ERROR' 
            , RECON_END_DTE=SYSDATE ,EXECUTION_ERROR =SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000)
            ,COMMENTS='recon engine execution failed ,please investigate through corresponding procedure '
            WHERE LOG_SK=PARAM_LOG_SK;
    COMMIT; 

END;
/
EXIT