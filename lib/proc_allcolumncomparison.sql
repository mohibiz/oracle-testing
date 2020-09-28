create or replace PROCEDURE proc_allcolumncomparion(executetargetoracledbrecon VARCHAR2 Default 'N' ,executesasrecon VARCHAR2 Default 'N' ,PARAM_LOG_SK NUMBER DEFAULT -1)
AS 
/*To Do's */
--Add handlers for known expception 
/*Potential Issues*/
    --ar_mutualcolumns may not populated correctly which could cause error records in ar_allcolumnerrors table.check proc_genreatemetadata to fix that.

 CURSOR tableCursor IS
	SELECT		TL.controltable_sk ,  TL.reconidentifier  ,TL.projectidentifier  ,TL.projectname,TL.TableIdentifier, TL.sourceoracledbSchema, TL.sourceoracledbTableName ,TL.CDBSchema, TL.CDBTableName ,TL.sasDATASETNAME,TL.targetoracledbRECON_IND,TL.sasRECON_IND
	,			TL.targetoracledbTableName, TL.TableKeys ,MCL.MutualColumnList,MCL.sasColumnList
	,			MCL.targetoracledbColumnList, MCL.sourceoracledbColumnList ,MCL.sourceoracledbColumnList CDBColumnList ,MCLL.sourceoracledbColumnAliasList CDBColumnAliasList
    ,           MCLL.TransformedColumnList , MCLL.sourceoracledbColumnAliasList  ,MCLL.targetoracledbColumnAliasList , MCLL.CompareLOBColumnList 
    ,           TL.DATAVAR , TL.Datenotin, TL.DateGreaterThan, TL.DateLessThan 
    ,           DECODE(dbms_lob.substr(MCLL.CompareLOBColumnList,3000) ,NULL , 'N','Y') LOB_TableFlag
    ,           LOBReconCondition
    ,           TL.EXTRACONDITION
	FROM		AR_ControlTable															TL
	LEFT OUTER JOIN	AR_MutualColumnList													MCL
    ON TL.controltable_sk=MCL.controltable_sk
    LEFT OUTER  JOIN AR_MutualColumnLobList MCLL
    ON TL.controltable_sk=MCLL.controltable_sk
;
condition_clause VARCHAR2(4000);
targetoracledbusername VARCHAR2(30); 
keycolumn1 VARCHAR2(10);
keycolumn2 VARCHAR2(10);
lobjoincondition VARCHAR2(500):=' ';

BEGIN

  SELECT user INTO targetoracledbusername FROM DUAL; 

IF executetargetoracledbrecon ='Y'  THEN
 ----------------------------------------------------------------------------------------------------  sourceoracledb to targetoracledb 
    FOR c IN tableCursor
	LOOP
		BEGIN
        DBMS_OUTPUT.PUT_LINE( 'table is '||c.sourceoracledbtablename || ' ' ||c.controltable_sk ||'lob flag-'||c.LOB_TableFlag||'targetoracledb recon flag-'||c.targetoracledbRECON_IND );

            condition_clause :=  case when  c.DATENOTIN IS NULL THEN ' AND 1=1 ' ELSE ' AND trunc('|| c.DATAVAR||') NOT IN '||c.DATENOTIN END
            ||case when  c.DateGreaterThan IS NULL THEN ' AND 1=1 ' ELSE ' AND ' || c.DATAVAR|| '>='||c.DateGreaterThan END
            ||case when  c.DateLessThan IS NULL THEN ' AND 1=1 ' ELSE ' AND ' ||c.DATAVAR|| '<='|| c.DateLessThan END
            ||case when  c.EXTRACONDITION IS NULL THEN ' AND 1=1 ' ELSE '  '||c.EXTRACONDITION END;
             DBMS_OUTPUT.PUT_LINE( c.controltable_sk  ||condition_clause);

            BEGIN
             lobjoincondition:=' ';
              for tablekeys in (select regexp_substr(C.TableKeys,'[^,]+', 1, level) as keycolumn from dual
                                    connect by regexp_substr(C.TableKeys, '[^,]+', 1, level) is not null) 
              loop
                lobjoincondition:=lobjoincondition||c.sourceoracledbtablename||'.'||tablekeys.keycolumn||'='||c.targetoracledbTableName||'.'||tablekeys.keycolumn||' AND ';
              end loop;
              lobjoincondition:=lobjoincondition||'1=1';
                    DBMS_OUTPUT.PUT_LINE(lobjoincondition); 
            END;
            
            

            IF c.LOB_TableFlag ='Y' AND c.targetoracledbRECON_IND='Y'
                THEN 
            UPDATE	AR_ALLCOLUMNRESULTS
			SET		InitiatedDateTime	=	LOCALTIMESTAMP
                    ,LOG_SK=PARAM_LOG_SK
		    WHERE	controltable_sk		=	c.controltable_sk;                      
                BEGIN
--                DBMS_OUTPUT.PUT_LINE(' CREATE TABLE '||c.sourceoracledbtablename||' AS SELECT * FROM ' ||c.sourceoracledbSCHEMA||'.'||c.sourceoracledbtablename||'@DBLINKsourceoracledb '||'WHERE '||condition_clause);   
                    EXECUTE IMMEDIATE ' CREATE TABLE '||c.sourceoracledbtablename||' AS SELECT * FROM ' ||c.sourceoracledbSCHEMA||'.'||c.sourceoracledbtablename||'@DBLINKsourceoracledb ';
                      
                    EXCEPTION WHEN others THEN NULL;
                END;
                BEGIN 

                EXECUTE IMMEDIATE ' DROP TABLE ART_sourceoracledbLOBRECONRESULTS'||c.controltable_sk  ;
                EXCEPTION WHEN others THEN NULL;
                END;

                EXECUTE IMMEDIATE --remove the hardcoding of the key from join condition for effective automation as key might for every LOB table
                   'CREATE TABLE ART_sourceoracledbLOBRECONRESULTS'||c.controltable_sk||' AS '
                    ||' SELECT '||c.targetoracledbCOLUMNALIASLIST||','||c.TRANSFORMEDCOLUMNLIST||c.COMPARELOBCOLUMNLIST
                    ||' FROM '||c.sourceoracledbtablename||' LEFT OUTER JOIN '||'sourceoracledb_SA1'||'.'||c.targetoracledbTableName
--                    ||' ON '||c.sourceoracledbtablename||'.i'||'='||c.targetoracledbTableName||'.i'||' AND '||c.sourceoracledbtablename||'.c'||'='||c.targetoracledbTableName||'.c'
                    ||' ON '||lobjoincondition
                    || CHR(10) ||		'WHERE 1=1 ' 
                    || CHR(10) ||		condition_clause
                    ;

               EXECUTE IMMEDIATE	
                    'UPDATE AR_ALLCOLUMNRESULTS'
				|| CHR(10) ||	'SET sourceoracledb_targetoracledb_MISMATCH = (SELECT COUNT(*) FROM ART_sourceoracledbLOBRECONRESULTS' || c.controltable_sk || ' WHERE 0'||c.LOBReconCondition||')'
				|| CHR(10) ||	', sourceoracledb_sas_MISMATCH = -1'
                || CHR(10) ||	', CompletedDateTime = LOCALTIMESTAMP'
				|| CHR(10) ||	', CompletedDuration = LOCALTIMESTAMP - InitiatedDateTime'
				|| CHR(10) ||	', SelectSQL = SelectSQL || CHR(10) ||	'' SELECT * FROM ART_sourceoracledbLOBRECONRESULTS' || c.controltable_sk || ' WHERE 0'||c.LOBReconCondition||' ORDER BY ' || c.TableKeys || ';/*sourceoracledb to targetoracledb*/'''
				|| CHR(10) ||	'WHERE controltable_sk = ''' || c.controltable_sk || ''''
			;
                 COMMIT;

                    BEGIN
                    EXECUTE IMMEDIATE  ' RENAME  '||c.sourceoracledbtablename|| ' TO '||SUBSTR('ET_'||C.sourceoracledbtablename,1,30)||c.controltable_sk  ;
                     DBMS_OUTPUT.PUT_LINE(' RENAME  '||c.sourceoracledbtablename|| ' TO '||SUBSTR('ET_'||C.sourceoracledbtablename,1,30)||c.controltable_sk);   
                    EXCEPTION WHEN others THEN NULL;
                    END;

        END IF;


        IF c.LOB_TableFlag ='N' AND c.targetoracledbRECON_IND='Y'
            THEN 
            UPDATE	AR_ALLCOLUMNRESULTS
			SET		InitiatedDateTime	=	LOCALTIMESTAMP
                    ,LOG_SK=PARAM_LOG_SK
		    WHERE	controltable_sk		=	c.controltable_sk;   

             BEGIN
				EXECUTE IMMEDIATE 'DROP TABLE ART_sourceoracledbtargetoracledbALLCOLRESULT' || c.controltable_sk;
				EXCEPTION WHEN others THEN NULL;
             END;


			EXECUTE IMMEDIATE	'CREATE TABLE ART_sourceoracledbtargetoracledbALLCOLRESULT' || c.controltable_sk || ' AS'
				|| CHR(10) ||	'WITH TableData AS'
				|| CHR(10) ||	'('
				|| CHR(10) ||		'SELECT /*+ PARALLEL(t,4) */ ''sourceoracledb'' TableSource, ''' || c.sourceoracledbSchema || ''' SchemaName, ' || c.sourceoracledbColumnList
				|| CHR(10) ||		'FROM ' || c.sourceoracledbSchema || '.' || c.sourceoracledbTableName || '@DBLinksourceoracledb'
                || CHR(10) ||		'WHERE 1=1 '
                || CHR(10) ||		condition_clause
				|| CHR(10) ||		'  UNION ALL'
				|| CHR(10) ||		'SELECT ''targetoracledb'' TableSource, ''sourceoracledb_SA1'' SchemaName, ' || c.MutualColumnList
				|| CHR(10) ||		'FROM sourceoracledb_SA1' || '.' || c.targetoracledbTableName
				|| CHR(10) ||		'WHERE 1=1 AND ADM_LATEST_IND = ''Y'''
				|| CHR(10) ||		'AND ADM_DELETED_IND <> ''Y'''
                || CHR(10) ||		condition_clause
				|| CHR(10) ||	')'
				|| CHR(10)
				|| CHR(10) ||	'SELECT /*+ PARALLEL(t,4) */ MIN(TableSource) TableSource, MIN(SchemaName) SchemaName, ' || c.MutualColumnList
				|| CHR(10) ||	'FROM TableData t'
				|| CHR(10) ||	'GROUP BY ' || c.MutualColumnList
				|| CHR(10) ||	'HAVING COUNT(*) <> 2'
				|| CHR(10) ||	'OR MIN(TableSource) = MAX(TableSource)'
			;
      	EXECUTE IMMEDIATE	
            'UPDATE AR_ALLCOLUMNRESULTS'
				|| CHR(10) ||	'SET sourceoracledb_targetoracledb_MISMATCH = (SELECT COUNT(*) FROM ART_sourceoracledbtargetoracledbALLCOLRESULT' || c.controltable_sk || ')'
				|| CHR(10) ||	', sourceoracledb_targetoracledb_Duplicates = (SELECT COUNT(*) FROM (SELECT 1 FROM ART_sourceoracledbtargetoracledbALLCOLRESULT' || c.controltable_sk || ' GROUP BY TableSource, ' || c.TableKeys || ' HAVING COUNT(*) > 1))'
                || CHR(10) ||	', sourceoracledb_sas_MISMATCH = -1'
				|| CHR(10) ||	', CompletedDateTime = LOCALTIMESTAMP'
				|| CHR(10) ||	', CompletedDuration = LOCALTIMESTAMP - InitiatedDateTime'
				|| CHR(10) ||	', SelectSQL = SelectSQL || CHR(10) ||	''SELECT * FROM ART_sourceoracledbtargetoracledbALLCOLRESULT' || c.controltable_sk || ' ORDER BY ' || c.TableKeys || ';/*sourceoracledb to targetoracledb*/'''
				|| CHR(10) ||	'WHERE controltable_sk = ''' || c.controltable_sk || ''''
			;
    END IF;

		EXCEPTION

		WHEN others THEN

            INSERT INTO AR_ALLCOLUMNERRORS(controltable_sk,reconidentifier,projectidentifier,projectname,TableIdentifier,RECONNAME, TableName, Message,log_sk)
			VALUES(c.controltable_sk,c.reconidentifier,c.projectidentifier,c.projectname,c.TableIdentifier,'sourceoracledb to targetoracledb', c.sourceoracledbTableName, SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 200),PARAM_LOG_SK);

			UPDATE	AR_ALLCOLUMNRESULTS
			SET		ErrorDateTime		=	LOCALTIMESTAMP
			WHERE	controltable_sk		=	c.controltable_sk;
    END;
    COMMIT;       
	END LOOP;
 DBMS_OUTPUT.PUT_LINE('completed sourceoracledb To targetoracledb Recon');   


    ----------------------------------------------------------------------------------------------------  CDB to targetoracledb 
FOR c IN tableCursor
LOOP
		BEGIN

         condition_clause :=  case when  c.DATENOTIN IS NULL THEN  ' AND 1=1  ' ELSE ' AND trunc('|| c.DATAVAR||') NOT IN '||c.DATENOTIN END
            ||case when  c.DateGreaterThan IS NULL THEN  ' AND 1=1 ' ELSE ' AND ' || c.DATAVAR|| '>='||c.DateGreaterThan END
            ||case when  c.DateLessThan IS NULL THEN  ' AND 1=1 ' ELSE ' AND ' ||c.DATAVAR|| '<='|| c.DateLessThan END
            ||case when  c.EXTRACONDITION IS NULL THEN ' AND 1=1 ' ELSE '  '||c.EXTRACONDITION END;

     
            BEGIN
                 lobjoincondition:=' ';
              for tablekeys in (select regexp_substr(C.TableKeys,'[^,]+', 1, level)as keycolumn from dual
                                    connect by regexp_substr(C.TableKeys, '[^,]+', 1, level) is not null) 
              loop
                  lobjoincondition:=lobjoincondition||c.CDBTableName||'.'||tablekeys.keycolumn||'='||c.targetoracledbTableName||'.'||tablekeys.keycolumn||' AND ';
              end loop;
              lobjoincondition:=lobjoincondition||'1=1';
                    DBMS_OUTPUT.PUT_LINE(lobjoincondition); 
            END;

            IF c.LOB_TableFlag ='Y'  AND c.targetoracledbRECON_IND='Y'
                THEN 

                BEGIN
                    EXECUTE IMMEDIATE ' CREATE TABLE '||c.CDBTableName||' AS SELECT * FROM ' ||c.CDBSchema||'.'||c.CDBTableName||'@DBLinkCDB '; --creating table locally as clob fields can not be selected over db link
                    EXCEPTION WHEN others THEN NULL;
                END;
                BEGIN 
                     EXECUTE IMMEDIATE ' DROP TABLE ART_CDBLOBRECONRESULTS'||c.controltable_sk  ;
                     EXCEPTION WHEN others THEN NULL;
                END;

                EXECUTE IMMEDIATE --remove the hardcoding of the key from join condition for effective automation as key might for every LOB table
                   'CREATE TABLE ART_CDBLOBRECONRESULTS'||c.controltable_sk||' AS '
                    ||' SELECT '||c.targetoracledbCOLUMNALIASLIST||','||c.TRANSFORMEDCOLUMNLIST||c.COMPARELOBCOLUMNLIST
                    ||' FROM '||c.CDBTableName||' LEFT OUTER JOIN '||'sourceoracledb_SA1'||'.'||c.targetoracledbTableName
--                    ||' ON '||c.CDBTableName||'.i'||'='||c.targetoracledbTableName||'.i'||' AND '||c.CDBTableName||'.c'||'='||c.targetoracledbTableName||'.c'
                    ||' ON '||lobjoincondition
                    || CHR(10) ||		'WHERE 1=1' --|| c.DATAVAR||'NOT IN '||c.DATENOTIN
                    || CHR(10) ||		condition_clause
                    ;

               EXECUTE IMMEDIATE	
                    'UPDATE AR_ALLCOLUMNRESULTS'
				|| CHR(10) ||	'SET CDB_targetoracledb_MISMATCH= (SELECT COUNT(*) FROM ART_CDBLOBRECONRESULTS' || c.controltable_sk || ' WHERE 0'||c.LOBReconCondition||')'
				|| CHR(10) ||	', CompletedDateTime = LOCALTIMESTAMP'
				|| CHR(10) ||	', CompletedDuration = LOCALTIMESTAMP - InitiatedDateTime'
				|| CHR(10) ||	', SelectSQL = SelectSQL || CHR(10) ||	'' SELECT * FROM ART_CDBLOBRECONRESULTS' || c.controltable_sk || ' WHERE 0'||c.LOBReconCondition||' ORDER BY ' || c.TableKeys || ';/*CDB to targetoracledb*/'''
		        || CHR(10) ||	'WHERE controltable_sk = ''' || c.controltable_sk || ''''
			;
                    BEGIN
--                    EXECUTE IMMEDIATE ' DROP TABLE '||c.CDBTableName  ; --droping the temoorary table after recon
                    EXECUTE IMMEDIATE  ' RENAME  '||c.CDBTableName|| ' TO '||SUBSTR('CT_'||C.sourceoracledbtablename,1,30)||c.controltable_sk  ;
                    EXCEPTION WHEN others THEN NULL;
                    END;

        END IF;


      IF c.LOB_TableFlag ='N' AND c.targetoracledbRECON_IND='Y'
            THEN 
             BEGIN
				EXECUTE IMMEDIATE 'DROP TABLE ART_CDBtargetoracledbALLCOLRESULT' || c.controltable_sk;
				EXCEPTION WHEN others THEN NULL;
             END;


			EXECUTE IMMEDIATE		'CREATE TABLE ART_CDBtargetoracledbALLCOLRESULT' || c.controltable_sk || ' AS'
				|| CHR(10) ||	'WITH TableData AS'
				|| CHR(10) ||	'('
				|| CHR(10) ||		'SELECT /*+ PARALLEL(t,4) */ ''CDB'' TableSource, ''' || c.CDBSchema || ''' SchemaName, ' || c.CDBColumnList
                || CHR(10) ||		', CASE WHEN ADM_OPERATION_CDE = ''D'' THEN ''delete'' WHEN ADM_OPERATION_CDE IN (''I'', ''U'') THEN ''create_update'' END ACTION'
				|| CHR(10) ||		'FROM ' || c.CDBSchema || '.' || c.CDBTableName || '@DBLinkCDB'
                || CHR(10) ||		' T WHERE 1=1 ' 
                || CHR(10) ||		condition_clause
				|| CHR(10) ||		'  UNION ALL'
				|| CHR(10) ||		'SELECT /*+ PARALLEL(t,4) */  ''targetoracledb'' TableSource, ''sourceoracledb_SA1'' SchemaName, ' || c.MutualColumnList
                || CHR(10) ||		', CASE WHEN ADM_DELETED_IND = ''Y'' THEN ''delete'' ELSE ''create_update'' END ACTION'
				|| CHR(10) ||		'FROM sourceoracledb_SA1' || '.' || c.targetoracledbTableName
				|| CHR(10) ||		' T WHERE 1=1 '
                || CHR(10) ||		condition_clause
			    || CHR(10) ||	')'
				|| CHR(10)
				|| CHR(10) ||	'SELECT /*+ PARALLEL(t,4) */ MIN(TableSource) TableSource, MIN(SchemaName) SchemaName, ' || c.MutualColumnList || ', ACTION'
				|| CHR(10) ||	'FROM TableData'
				|| CHR(10) ||	'GROUP BY ' || c.MutualColumnList || ', ACTION'
				|| CHR(10) ||	'HAVING COUNT(*) = 1'
			;
			EXECUTE IMMEDIATE	
            'UPDATE AR_ALLCOLUMNRESULTS'
				|| CHR(10) ||	'SET CDB_targetoracledb_MISMATCH = (SELECT COUNT(*) FROM ART_CDBtargetoracledbALLCOLRESULT' || c.controltable_sk || ')'
				|| CHR(10) ||	', CompletedDateTime = LOCALTIMESTAMP'
				|| CHR(10) ||	', CompletedDuration = LOCALTIMESTAMP - InitiatedDateTime'
				|| CHR(10) ||	', SelectSQL = SelectSQL || CHR(10) ||	''SELECT * FROM ART_CDBtargetoracledbALLCOLRESULT' || c.controltable_sk || ' ORDER BY ' || c.TableKeys || ';/*CDB to targetoracledb*/'''
				|| CHR(10) ||	'WHERE controltable_sk = ''' || c.controltable_sk || ''''
			;

    end if;
		EXCEPTION
        	WHEN others THEN

           INSERT INTO AR_ALLCOLUMNERRORS(controltable_sk,reconidentifier,projectidentifier,projectname,TableIdentifier,RECONNAME, TableName, Message,log_sk)
			VALUES(c.controltable_sk,c.reconidentifier,c.projectidentifier,c.projectname,c.TableIdentifier,'CDB to targetoracledb', c.sourceoracledbTableName, SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 200),PARAM_LOG_SK);

			UPDATE	AR_ALLCOLUMNRESULTS
			SET		ErrorDateTime		=	LOCALTIMESTAMP
			WHERE	controltable_sk		=	c.controltable_sk;

      END;       
	END LOOP;
      DBMS_OUTPUT.PUT_LINE('completed CDB To targetoracledb Recon');   
 END IF; -- Ending executetargetoracledbrecon='Y' condition

    ----------------------------------------------------------------------------------------------------  sourceoracledb to sas 
  IF executesasrecon='Y' THEN
    FOR c IN tableCursor
	LOOP
		BEGIN
        IF (check_object_exists(targetoracledbusername,'TABLE',c.sasDATASETNAME)=1) THEN

            condition_clause :=  case when  c.DATENOTIN IS NULL THEN ' AND 1=1 ' ELSE ' AND trunc('|| c.DATAVAR||') NOT IN '||c.DATENOTIN END
            ||case when  c.DateGreaterThan IS NULL THEN ' AND 1=1 ' ELSE ' AND ' || c.DATAVAR|| '>='||c.DateGreaterThan END
            ||case when  c.DateLessThan IS NULL THEN ' AND 1=1 ' ELSE ' AND ' ||c.DATAVAR|| '<='|| c.DateLessThan END
            ||case when  c.EXTRACONDITION IS NULL THEN ' AND 1=1 ' ELSE '  '||c.EXTRACONDITION END;

           DBMS_OUTPUT.PUT_LINE( c.controltable_sk  ||condition_clause);

        IF c.LOB_TableFlag ='N' AND c.sasRECON_IND='Y'

            THEN 

            UPDATE	AR_ALLCOLUMNRESULTS
			SET		InitiatedDateTime	=	LOCALTIMESTAMP
                    ,LOG_SK=PARAM_LOG_SK
		    WHERE	controltable_sk		=	c.controltable_sk;  

             BEGIN
				EXECUTE IMMEDIATE 'DROP TABLE ART_sourceoracledbsasALLCOLRESULT' || c.controltable_sk;
				EXCEPTION WHEN others THEN NULL;
             END;



			EXECUTE IMMEDIATE  'CREATE TABLE ART_sourceoracledbsasALLCOLRESULT' || c.controltable_sk || ' AS'
				|| CHR(10) ||	'WITH TableData AS'
				|| CHR(10) ||	'('
				|| CHR(10) ||		'SELECT /*+ PARALLEL(t,4) */ ''sourceoracledb'' TableSource, ''' || c.sourceoracledbSchema || ''' SchemaName, ' || c.sourceoracledbColumnList
				|| CHR(10) ||		'FROM ' || c.sourceoracledbSchema || '.' || c.sourceoracledbTableName || '@DBLinksourceoracledb'
                || CHR(10) ||		'WHERE 1=1 '
                || CHR(10) ||		condition_clause
				|| CHR(10) ||		'  UNION ALL'
				|| CHR(10) ||		'SELECT ''sas'' TableSource,  ''' || targetoracledbusername|| '''  SchemaName, ' || c.sasColumnList
				|| CHR(10) ||		'FROM '|| targetoracledbusername|| '.' || c.sasDATASETNAME 
				|| CHR(10) ||		'WHERE 1=1 '
			    || CHR(10) ||		condition_clause
				|| CHR(10) ||	')'
				|| CHR(10)
				|| CHR(10) ||	'SELECT /*+ PARALLEL(t,4) */ MIN(TableSource) TableSource, MIN(SchemaName) SchemaName, ' || c.MutualColumnList
				|| CHR(10) ||	'FROM TableData t'
				|| CHR(10) ||	'GROUP BY ' || c.MutualColumnList
				|| CHR(10) ||	'HAVING COUNT(*) <> 2'
				|| CHR(10) ||	'OR MIN(TableSource) = MAX(TableSource)' 
			;
      	EXECUTE IMMEDIATE	
            'UPDATE AR_ALLCOLUMNRESULTS'
				|| CHR(10) ||	'SET sourceoracledb_sas_MISMATCH = (SELECT COUNT(*) FROM ART_sourceoracledbsasALLCOLRESULT' || c.controltable_sk || ')'
				|| CHR(10) ||	', sourceoracledb_targetoracledb_MISMATCH = -1'
                || CHR(10) ||	', sourceoracledb_targetoracledb_Duplicates = -1'
                || CHR(10) ||	', CDB_targetoracledb_MISMATCH = -1'
                || CHR(10) ||	', CompletedDateTime = LOCALTIMESTAMP'
				|| CHR(10) ||	', CompletedDuration = LOCALTIMESTAMP - InitiatedDateTime'
				|| CHR(10) ||	', SelectSQL = SelectSQL || CHR(10) ||	''SELECT * FROM ART_sourceoracledbsasALLCOLRESULT' || c.controltable_sk || ' ORDER BY ' || c.TableKeys || ';/*sourceoracledb to sas*/'''
				|| CHR(10) ||	'WHERE controltable_sk = ''' || c.controltable_sk || ''''
			;

    END IF;
ELSE 
 INSERT INTO AR_ALLCOLUMNERRORS(controltable_sk,reconidentifier,projectidentifier,projectname,TableIdentifier,RECONNAME, TableName, Message,log_sk)
			VALUES(c.controltable_sk,c.reconidentifier,c.projectidentifier,c.projectname,c.TableIdentifier,'sourceoracledb to sas', c.sourceoracledbTableName, 'table did not get copied from sas',PARAM_LOG_SK);
END IF;

		EXCEPTION

		WHEN others THEN

            INSERT INTO AR_ALLCOLUMNERRORS(controltable_sk,reconidentifier,projectidentifier,projectname,TableIdentifier,RECONNAME, TableName, Message,log_sk)
			VALUES(c.controltable_sk,c.reconidentifier,c.projectidentifier,c.projectname,c.TableIdentifier,'sourceoracledb to sas', c.sourceoracledbTableName, SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 500),PARAM_LOG_SK);

			UPDATE	AR_ALLCOLUMNRESULTS
			SET		ErrorDateTime		=	LOCALTIMESTAMP
			WHERE	controltable_sk		=	c.controltable_sk;
    END;
    COMMIT;       
	END LOOP;
 DBMS_OUTPUT.PUT_LINE('completed sourceoracledb To sas Recon');   
END IF; -- Ending executesasrecon='Y' condition

    EXCEPTION
   WHEN others THEN
        UPDATE AR_execution_log
        SET 
        execution_error =SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000)
        ,comments='reccon engine execution failed ,please investigate through corresponding procedure '
        WHERE PROCEDURE_NME='PROC_ALLCOLUMNCOMPARION' AND LOG_SK=PARAM_LOG_SK;
DBMS_OUTPUT.PUT_LINE('completed CDB To targetoracledb Recon');

END;
/