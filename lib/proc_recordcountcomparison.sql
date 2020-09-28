CREATE OR REPLACE PROCEDURE proc_recordcountcomparison(executetargetoracledbrecon VARCHAR2 Default 'N' ,executesasrecon VARCHAR2 Default 'N' ,PARAM_LOG_SK NUMBER DEFAULT -1)
AS 
/*To Do's */
--Create single procedure to get the record count and call that procedure for sourceoracledb, cdb and targetoracledb
--repeatitive code must be replaced with genric parameterized procedure.Should be done at next best avaible time.
CURSOR tableCursor1 IS
	SELECT		TL.controltable_sk ,  TL.reconidentifier  ,TL.projectidentifier  ,TL.projectname,TL.TableIdentifier
    ,TL.sourceoracledbSchema , TL.sourceoracledbTableName  ,TL.CDBSchema ,TL.CDBTableName , TL.TableKeys
	,			TL.DATAVAR , TL.Datenotin, TL.DateGreaterThan, TL.DateLessThan ,TL.EXTRACONDITION
	FROM		AR_controltable TL											

;
/*CURSOR tableCursor2 IS
	SELECT		TL.controltable_sk ,  TL.reconidentifier  ,TL.projectidentifier  ,TL.projectname,TL.TableIdentifier
                ,TL.CDBSchema SchemaName, TL.CDBTableName TableName, TL.TableKeys
	,			TL.DATAVAR ,TL.Datenotin, TL.DateGreaterThan, TL.DateLessThan,TL.EXTRACONDITION
	FROM		AR_controltable TL 		
												
;*/
CURSOR tableCursor3 IS
	SELECT		TL.controltable_sk ,  TL.reconidentifier  ,TL.projectidentifier  ,TL.projectname,TL.TableIdentifier
    ,targetoracledbSchema SchemaName, TL.targetoracledbTableName TableName
	,			TL.DATAVAR ,TL.Datenotin, TL.DateGreaterThan, TL.DateLessThan ,TL.EXTRACONDITION
	FROM		AR_controltable TL 	WHERE targetoracledbRECON_IND='Y'
												
;

CURSOR tableCursor4 IS
	SELECT		TL.controltable_sk ,  TL.reconidentifier  ,TL.projectidentifier  ,TL.projectname,TL.TableIdentifier
    ,' ' SchemaName, TL.sasDATASETNAME TableName --Schema name must come from userconfig file because sasDATaset created as temp table in the schema where the scripts are running.if not implemented .report it
	,			TL.DATAVAR ,TL.Datenotin, TL.DateGreaterThan, TL.DateLessThan ,TL.EXTRACONDITION
	FROM		AR_controltable TL 	WHERE sasRECON_IND='Y'
												
;
condition_clause VARCHAR2(4000);
count_load_check NUMBER;
targetoracledbusername VARCHAR2(30);
BEGIN
SELECT user INTO targetoracledbusername FROM DUAL;

IF executetargetoracledbrecon ='Y' OR executesasrecon='Y' THEN
----------------------------------------------------------------------------------------------------  sourceoracledb And CDB Tables - Update Record Count	
	FOR c IN tableCursor1 LOOP
		BEGIN
		SELECT count(*) into count_load_check FROM AR_RECORDCOUNTRESULTS WHERE controltable_sk = c.controltable_sk and LOG_SK IS NULL;
		IF (count_load_check=1) THEN 
			UPDATE	AR_RECORDCOUNTRESULTS
			SET		InitiatedDateTime	=	LOCALTIMESTAMP
            ,		LOG_SK	=	PARAM_LOG_SK
			WHERE	controltable_sk 		=	c.controltable_sk;
            
            condition_clause :=  case when  c.DATENOTIN IS NULL THEN ' AND 1=1 ' ELSE ' AND trunc('|| c.DATAVAR||') NOT IN '||c.DATENOTIN END
            ||case when  c.DateGreaterThan IS NULL THEN ' AND 1=1 ' ELSE ' AND trunc(' || c.DATAVAR|| ')>=to_date('||c.DateGreaterThan||',''dd/mm/yyyy'') ' END
            ||case when  c.DateLessThan IS NULL THEN ' AND 1=1 ' ELSE ' AND trunc(' || c.DATAVAR|| ')<=to_date('||c.DateLessThan||',''dd/mm/yyyy'') ' END 
            ||case when  c.EXTRACONDITION IS NULL THEN ' AND 1=1 ' ELSE '  '||c.EXTRACONDITION END;
            
            
                                      
			EXECUTE IMMEDIATE	'UPDATE AR_RECORDCOUNTRESULTS'
				|| CHR(10) ||	'SET sourceoracledbRecordCount = ('
				|| CHR(10) ||		'SELECT /*+ PARALLEL(t,4) */ COUNT(*) FROM ' || c.sourceoracledbSchema || '.' || c.sourceoracledbTableName || '@DBLinksourceoracledb T'
				|| CHR(10) ||		'WHERE 1=1  ' 
				|| CHR(10) ||		condition_clause
				|| CHR(10) ||	') '
                || CHR(10) ||	'WHERE controltable_sk = ''' || c.controltable_sk || ''''
			;
            
			EXECUTE IMMEDIATE	'UPDATE AR_RECORDCOUNTRESULTS'
				|| CHR(10) ||	'SET CDBRecordCount = ('
				|| CHR(10) ||		'WITH A AS (SELECT /*+ PARALLEL(t,4) */ ' || c.TableKeys || ', ADM_OPERATION_CDE'
				|| CHR(10) ||		', ROW_NUMBER() OVER(PARTITION BY ' || c.TableKeys || ' ORDER BY ADM_SOURCE_COMMIT_DTS DESC) top_rank'
				|| CHR(10) ||		'FROM ' || c.CDBSchema || '.' || c.CDBTableName || '@DBLinkCDB T'
               || CHR(10) ||		'WHERE 1=1  '
				|| CHR(10) ||		condition_clause
				|| CHR(10) ||	')'
				|| CHR(10) ||	'SELECT COUNT(*) FROM A'
				|| CHR(10) ||	'WHERE ADM_OPERATION_CDE <> ''D'''
				|| CHR(10) ||	'AND top_rank = 1)'
                || CHR(10) ||	'WHERE controltable_sk = ''' || c.controltable_sk || ''''
			;
            
			UPDATE	AR_RECORDCOUNTRESULTS
			SET		CompletedDateTime	=	LOCALTIMESTAMP
			,		CompletedDuration	=	LOCALTIMESTAMP - InitiatedDateTime
        	WHERE	controltable_sk 		=	c.controltable_sk;
          END IF;--Ending count_load_check=1
		EXCEPTION
		WHEN others THEN 

			INSERT INTO AR_RECORDCOUNTERRORS(controltable_sk,reconidentifier,projectidentifier,projectname,TableIdentifier,TableSource, TableName, Message,LOG_SK)
			VALUES(c.controltable_sk,c.reconidentifier,c.projectidentifier,c.projectname,c.TableIdentifier,'sourceoracledb and CDB', c.sourceoracledbTableName, SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 200),PARAM_LOG_SK);
            
			UPDATE	AR_RECORDCOUNTRESULTS
			SET		ErrorDateTime		=	LOCALTIMESTAMP
	        WHERE	controltable_sk 		=	c.controltable_sk;
		END;
        COMMIT;
	END LOOP;


END IF; --Ending condition executetargetoracledbrecon ='Y' OR executesasrecon='Y'
----------------------------------------------------------------------------------------------------  targetoracledb Tables - Update Record Count
IF executetargetoracledbrecon='Y' THEN
	FOR c IN tableCursor3 LOOP
		BEGIN
			UPDATE	AR_RECORDCOUNTRESULTS
			SET		InitiatedDateTime	=	LOCALTIMESTAMP
            ,		LOG_SK	=	PARAM_LOG_SK
			WHERE	controltable_sk 		=	c.controltable_sk;
            
           condition_clause :=  case when  c.DATENOTIN IS NULL THEN ' AND 1=1 ' ELSE ' AND trunc('|| c.DATAVAR||') NOT IN '||c.DATENOTIN END
            ||case when  c.DateGreaterThan IS NULL THEN ' AND 1=1 ' ELSE ' AND trunc(' || c.DATAVAR|| ')>=to_date('||c.DateGreaterThan||',''dd/mm/yyyy'') ' END
            ||case when  c.DateLessThan IS NULL THEN ' AND 1=1 ' ELSE ' AND trunc(' || c.DATAVAR|| ')<=to_date('||c.DateLessThan||',''dd/mm/yyyy'') ' END 
            ||case when  c.EXTRACONDITION IS NULL THEN ' AND 1=1 ' ELSE '  '||c.EXTRACONDITION END;
            
			
			EXECUTE IMMEDIATE	'UPDATE AR_RECORDCOUNTRESULTS'
				|| CHR(10) ||	'SET targetoracledbRecordCount = ('
				|| CHR(10) ||		'SELECT /*+ PARALLEL(t,4) */ COUNT(*) FROM ' || c.SchemaName || '.' || c.TableName
				|| CHR(10) ||		'WHERE 1=1 and adm_latest_ind = ''Y'''
				|| CHR(10) ||		'AND adm_deleted_ind <> ''Y'''
                || CHR(10) ||		condition_clause
				|| CHR(10) ||	')'
                || CHR(10) ||	'WHERE controltable_sk = ''' || c.controltable_sk || ''''
			;
			
			UPDATE	AR_RECORDCOUNTRESULTS
			SET		CompletedDateTime	=	LOCALTIMESTAMP
			,		CompletedDuration	=	LOCALTIMESTAMP - InitiatedDateTime
            WHERE	controltable_sk 		=	c.controltable_sk;
		EXCEPTION
		WHEN others THEN
		
            INSERT INTO AR_RECORDCOUNTERRORS(controltable_sk,reconidentifier,projectidentifier,projectname,TableIdentifier,TableSource, TableName, Message,LOG_SK)
			VALUES(c.controltable_sk,c.reconidentifier,c.projectidentifier,c.projectname,c.TableIdentifier,'targetoracledb_' || c.SchemaName, c.TableName, SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 200),PARAM_LOG_SK);
            
			UPDATE	AR_RECORDCOUNTRESULTS
			SET		ErrorDateTime		=	LOCALTIMESTAMP
			  WHERE	controltable_sk 		=	c.controltable_sk;
		END;
        COMMIT;
	END LOOP;
 END IF;  
    ----------------------------------------------------------------------------------------------------  sas Tables - Update Record Count
IF executesasrecon='Y' THEN
FOR c IN tableCursor4 LOOP
		BEGIN
			UPDATE	AR_RECORDCOUNTRESULTS
			SET		InitiatedDateTime	=	LOCALTIMESTAMP
            ,		LOG_SK	=	PARAM_LOG_SK
			WHERE	controltable_sk 		=	c.controltable_sk;
            
            condition_clause :=  case when  c.DATENOTIN IS NULL THEN ' AND 1=1 ' ELSE ' AND trunc('|| c.DATAVAR||') NOT IN '||c.DATENOTIN END
            ||case when  c.DateGreaterThan IS NULL THEN ' AND 1=1 ' ELSE ' AND trunc(' || c.DATAVAR|| ')>=to_date('||c.DateGreaterThan||',''dd/mm/yyyy'') ' END
            ||case when  c.DateLessThan IS NULL THEN ' AND 1=1 ' ELSE ' AND trunc(' || c.DATAVAR|| ')<=to_date('||c.DateLessThan||',''dd/mm/yyyy'') ' END 
            ||case when  c.EXTRACONDITION IS NULL THEN ' AND 1=1 ' ELSE '  '||c.EXTRACONDITION END;
            
            
                                    
			execute immediate	'UPDATE AR_RECORDCOUNTRESULTS'
				|| CHR(10) ||	'SET sasdatasetrecordcount = ('
				|| CHR(10) ||		'SELECT /*+ PARALLEL(t,4) */ COUNT(*) FROM ' || targetoracledbusername || '.' || c.TableName
				|| CHR(10) ||		'WHERE 1=1  ' 
				|| CHR(10) ||		condition_clause
				|| CHR(10) ||	') '
                || CHR(10) ||	'WHERE controltable_sk = ''' || c.controltable_sk || ''''
			;
			
			UPDATE	AR_RECORDCOUNTRESULTS
			SET		CompletedDateTime	=	LOCALTIMESTAMP
			,		CompletedDuration	=	LOCALTIMESTAMP - InitiatedDateTime
        	WHERE	controltable_sk 		=	c.controltable_sk;
            
		EXCEPTION
		WHEN others THEN 

			INSERT INTO AR_RECORDCOUNTERRORS(controltable_sk,reconidentifier,projectidentifier,projectname,TableIdentifier,TableSource, TableName, Message,LOG_SK)
			VALUES(c.controltable_sk,c.reconidentifier,c.projectidentifier,c.projectname,c.TableIdentifier,'sas', c.TableName, SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 200),PARAM_LOG_SK);
            
			UPDATE	AR_RECORDCOUNTRESULTS
			SET		ErrorDateTime		=	LOCALTIMESTAMP
	        WHERE	controltable_sk 		=	c.controltable_sk;
		END;
        COMMIT;
	END LOOP;
END IF;
----------------------------------------------------------------------------------------------------  Calculate Record Count Difference
UPDATE	AR_RECORDCOUNTRESULTS
SET		sourceoracledbtargetoracledb_RecordCountDifference = sourceoracledbRecordCount - targetoracledbRecordCount
,sourceoracledbsas_RecordCountDifference = sourceoracledbRecordCount - sasDatasetRecordCount ;

COMMIT;
EXCEPTION 
		WHEN others THEN
        UPDATE AR_execution_log
        SET 
        execution_error =SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000)
        ,comments='reccon engine execution failed ,please investigate through corresponding procedure '
        WHERE PROCEDURE_NME='PROC_RECORDCOUNTCOMPARISON' AND LOG_SK=PARAM_LOG_SK;
  
    END;
  /
 --exec  proc_recordcountcomparison('Y','N',100);

  




