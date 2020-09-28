create or replace PROCEDURE proc_renamesastempdataset(executetargetoracledbrecon VARCHAR2 Default 'N' ,executesasrecon VARCHAR2 Default 'N' ,PARAM_LOG_SK NUMBER DEFAULT -1  )
AS 
targetoracledbusername VARCHAR2(30); 
CURSOR tableCursor IS
	SELECT		TL.controltable_sk ,  TL.reconidentifier  ,TL.projectidentifier  ,TL.projectname,TL.TableIdentifier
    ,TL.sourceoracledbSchema , TL.sourceoracledbTableName  ,TL.CDBSchema ,TL.CDBTableName , TL.sasDATASETNAME
	FROM		AR_controltable TL			WHERE 	sasRECON_IND='Y'	;
    BEGIN
    SELECT user INTO targetoracledbusername FROM DUAL; 
    
    IF  executesasrecon='Y'    THEN 
          FOR c IN tableCursor LOOP
                BEGIN
                        IF (check_object_exists(targetoracledbusername,'TABLE',c.sasDATASETNAME)=1) THEN
                                EXECUTE IMMEDIATE  ' RENAME  '||c.sasDATASETNAME|| ' TO '||SUBSTR('T_'||C.sasDATASETNAME,1,30)||c.controltable_sk  ; --rename the table after recon so that it can be used for analysis and cleanedup.
                        END IF;
                         
                END;    
        END LOOP;
    END IF;

    EXCEPTION 
		WHEN others THEN
        UPDATE AR_execution_log
        SET 
        execution_error =SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000)
        ,comments='reccon engine execution failed ,please investigate through corresponding procedure '
        WHERE PROCEDURE_NME='PROC_RENAMEsasTEMPDATASET' AND LOG_SK=PARAM_LOG_SK;

    END;
