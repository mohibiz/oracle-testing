CREATE OR REPLACE PROCEDURE proc_loadcommonmetadata(exestructurecrecon VARCHAR2 Default 'N' ,exerowcountcrecon VARCHAR2 Default 'N' , exeallcolumncrecon VARCHAR2 Default 'N' ,executetargetoracledbrecon VARCHAR2 Default 'N' ,executesasrecon VARCHAR2 Default 'N' ,PARAM_LOG_SK NUMBER DEFAULT -1 )
IS 

/* To Do's
     exception handling  */
     
    control_condition VARCHAR2(100);
    --nested procedure , not recomondede,needs to be replaced later using proper package implementations 
    PROCEDURE LOAD_CONTROLTABLE(control_condition VARCHAR2)
    AS
    BEGIN
    EXECUTE IMMEDIATE  'TRUNCATE TABLE ar_controltable';
   EXECUTE IMMEDIATE  'INSERT INTO ar_controltable
   (
        controltable_sk,
        reconidentifier,
        projectidentifier,
        projectname,
        tableidentifier,
        sourceoracledbschema,
        sourceoracledbtablename,
        cdbschema,
        cdbtablename,
        targetoracledbschema,
        targetoracledbtablename,
        saslib,
        sasDATASETNAME,
        tablekeys,
        datavar,
        dategreaterthan,
        datelessthan,
        datenotin,
        EXTRACONDITION,
        EXCLUDCOLUMNLIST,
        targetoracledbrecon_ind,
        sasrecon_ind,
        comments
   ) 
    SELECT 
        SEQ_CONTROLTABLE.nextval controltable_sk,
        reconidentifier,
        projectidentifier,
        projectname,
        tableidentifier,
        sourceoracledbschema,
        sourceoracledbtablename,
        cdbschema,
        cdbtablename,
        targetoracledbschema,
        targetoracledbtablename,
        saslib,
        sasDATASETNAME,
        tablekeys,
        datavar,
        dategreaterthan,
        datelessthan,
        datenotin,
        EXTRACONDITION,
        EXCLUDCOLUMNLIST,
        targetoracledbrecon_ind,
        sasrecon_ind,
        comments
    FROM
        ar_reconcontroltablecsv WHERE 1=1 and '||control_condition;
    COMMIT;
        END ;
      
BEGIN 


                           -----------------------Load metadata----------------------

IF( executetargetoracledbrecon='Y' AND executesasrecon='N' ) THEN control_condition := 'targetoracledbrecon_ind=''Y''';
    ELSIF (executetargetoracledbrecon='N' AND executesasrecon='Y') THEN  control_condition := 'sasrecon_ind=''Y''';
    ELSIF (executetargetoracledbrecon='Y' AND executesasrecon='Y') THEN control_condition := 'targetoracledbrecon_ind =''Y'' OR sasrecon_ind=''Y''  ';
    ELSE control_condition :='1=2';
END IF;


IF (executetargetoracledbrecon='Y' OR executesasrecon='Y') --do not load metadta for the resulsts table untless condition is passed
THEN
    --caliing nested procedure to load control table based on inputs from config and csv files ,This truncates and reloads control_ tables
    LOAD_CONTROLTABLE(control_condition);
    --Truncating mutual colums everytime as it does'nt require to store history as results can be analysed from ART_% tables 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE AR_MUTUALCOLUMNS';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE AR_MUTUALCOLUMNLIST';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE AR_MUTUALCOLUMNLOBS';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE AR_MUTUALCOLUMNLOBLIST';
    
          IF exestructurecrecon='Y'  THEN
            MERGE INTO AR_STRUCTURERESULTS olddata
            USING (SELECT		   
                        CONTROLTABLE_SK
            ,           RECONIDENTIFIER
            ,           PROJECTIDENTIFIER
            ,           PROJECTNAME
            ,           TABLEIDENTIFIER
            ,			sourceoracledbTableName
            ,		    CDBTableName
            ,			targetoracledbTABLENAME
            ,			sasDATASETNAME
            ,			CAST(NULL AS NUMBER)												sourceoracledb_targetoracledb_column_mismatch
            ,			CAST(NULL AS NUMBER)												sourceoracledb_targetoracledb_datetype_mismatch
            ,			CAST(NULL AS NUMBER)												sourceoracledb_targetoracledb_datalength_mismatch
            ,			CAST(NULL AS NUMBER)												sourceoracledb_targetoracledb_data_prec_mismatch
            ,			CAST(NULL AS NUMBER)												cdb_targetoracledb_column_mismatch
            ,			CAST(NULL AS NUMBER)												cdb_targetoracledb_datetype_mismatch
            ,			CAST(NULL AS NUMBER)												cdb_targetoracledb_datalength_mismatch
            ,			CAST(NULL AS NUMBER)												cdb_targetoracledb_data_prec_mismatch
            ,			CAST(NULL AS TIMESTAMP)												InitiatedDateTime
            ,			CAST(NULL AS TIMESTAMP)												CompletedDateTime
            ,			CAST(NULL AS INTERVAL DAY TO SECOND)								CompletedDuration
            ,			CAST(NULL AS TIMESTAMP)												ErrorDateTime
            ,			CAST('--' || sourceoracledbTableName AS VARCHAR2(4000))						SelectSQL
            ,           CAST(NULL AS NUMBER)                                                LOG_SK
            FROM		ar_controltable ) newdata
            ON (olddata.controltable_sk=newdata.controltable_sk)
            when MATCHED then update set  olddata.CompletedDuration=newdata.CompletedDuration where 1=2
            when NOT MATCHED THEN
                INSERT  ( 
                    olddata.controltable_sk,
                    olddata.reconidentifier,
                    olddata.projectidentifier,
                    olddata.projectname,
                    olddata.tableidentifier,
                    olddata.sourceoracledbtablename,
                    olddata.cdbtablename,
                    olddata.targetoracledbtablename,
                    olddata.sasDATASETNAME,
                    olddata.sourceoracledb_targetoracledb_column_mismatch,
                    olddata.sourceoracledb_targetoracledb_datetype_mismatch,
                    olddata.sourceoracledb_targetoracledb_datalength_mismatch,
                    olddata.sourceoracledb_targetoracledb_data_prec_mismatch,
                    olddata.cdb_targetoracledb_column_mismatch,
                    olddata.cdb_targetoracledb_datetype_mismatch,
                    olddata.cdb_targetoracledb_datalength_mismatch,
                    olddata.cdb_targetoracledb_data_prec_mismatch,
                    olddata.initiateddatetime,
                    olddata.completeddatetime,
                    olddata.completedduration,
                    olddata.errordatetime,
                    olddata.selectsql,
                    olddata.LOG_SK) 
            VALUES
            (
                        newdata.controltable_sk
            ,           newdata.reconidentifier
            ,           newdata.projectidentifier
            ,           newdata.projectname
            ,           newdata.TableIdentifier
            ,			newdata.sourceoracledbTableName
            ,			newdata.CDBTableName
            ,			newdata.targetoracledbTABLENAME
            ,           newdata.sasDATASETNAME
            ,			CAST(NULL AS NUMBER)												
            ,			CAST(NULL AS NUMBER)												
            ,			CAST(NULL AS NUMBER)												
            ,			CAST(NULL AS NUMBER)												
            ,			CAST(NULL AS NUMBER)												
            ,			CAST(NULL AS NUMBER)												
            ,			CAST(NULL AS NUMBER)												
            ,			CAST(NULL AS NUMBER)												
            ,			CAST(NULL AS TIMESTAMP)												
            ,			CAST(NULL AS TIMESTAMP)												
            ,			CAST(NULL AS INTERVAL DAY TO SECOND)								
            ,			CAST(NULL AS TIMESTAMP)												
            ,			CAST('--' || newdata.sourceoracledbTableName AS VARCHAR2(4000))	
            ,           CAST(NULL AS NUMBER)
            );
        end if;    
            
        COMMIT;
         IF exeallcolumncrecon='Y' THEN
        MERGE INTO  AR_ALLCOLUMNRESULTS olddata
        USING 
        (
        SELECT		   
                        controltable_sk
            ,           reconidentifier
            ,           projectidentifier
            ,           projectname
            ,           TableIdentifier
            ,			sourceoracledbTableName
            ,			CDBTableName
            ,			targetoracledbTABLENAME
            ,			sasDATASETNAME
            ,			CAST(NULL AS NUMBER)												sourceoracledb_targetoracledb_MISMATCH
            ,			CAST(NULL AS NUMBER)												sourceoracledb_targetoracledb_Duplicates
            ,			CAST(NULL AS NUMBER)												CDB_targetoracledb_MISMATCH
            ,           CAST(NULL AS NUMBER)                                                sourceoracledb_sas_MISMATCH
            ,			CAST(NULL AS TIMESTAMP)												InitiatedDateTime
            ,			CAST(NULL AS TIMESTAMP)												CompletedDateTime
            ,			CAST(NULL AS INTERVAL DAY TO SECOND)								CompletedDuration
            ,			CAST(NULL AS TIMESTAMP)												ErrorDateTime
            ,			CAST('--' || sourceoracledbTableName AS VARCHAR2(4000))						SelectSQL
            ,			CAST(NULL AS NUMBER)	                                            log_sk
            FROM		ar_controltable 
        ) newdata
        ON (olddata.controltable_sk=newdata.controltable_sk)
            when MATCHED then update set  olddata.reconidentifier=newdata.reconidentifier where 1=2
            when NOT MATCHED THEN
                INSERT 
                (
            controltable_sk,
            reconidentifier,
            projectidentifier,
            projectname,
            tableidentifier,
            sourceoracledbtablename,
            cdbtablename,
            targetoracledbtablename,
            sasDATASETNAME,
            sourceoracledb_targetoracledb_MISMATCH,
            sourceoracledb_targetoracledb_duplicates,
            CDB_targetoracledb_MISMATCH,
            sourceoracledb_sas_MISMATCH,
            initiateddatetime,
            completeddatetime,
            completedduration,
            errordatetime,
            selectsql,
            log_sk
                )
                VALUES
                (
            newdata.controltable_sk,
            newdata.reconidentifier,
            newdata.projectidentifier,
            newdata.projectname,
            newdata.tableidentifier,
            newdata.sourceoracledbtablename,
            newdata.cdbtablename,
            newdata.targetoracledbtablename,
            newdata.sasDATASETNAME,
            newdata.sourceoracledb_targetoracledb_MISMATCH,
            newdata.sourceoracledb_targetoracledb_duplicates,
            newdata.CDB_targetoracledb_MISMATCH,
            newdata.sourceoracledb_sas_MISMATCH,
            newdata.initiateddatetime,
            newdata.completeddatetime,
            newdata.completedduration,
            newdata.errordatetime,
            newdata.selectsql,
            newdata.log_sk
                );
        COMMIT;
        END IF;


        IF exerowcountcrecon='Y' THEN
        MERGE INTO AR_RECORDCOUNTRESULTS olddata
        USING 
        (
        SELECT		
                        controltable_sk
            ,           reconidentifier
            ,           projectidentifier
            ,           projectname
            ,           TableIdentifier
            ,           sourceoracledbTableName
            ,			CDBTableName
            ,			targetoracledbTABLENAME												targetoracledbTableName
            ,			CAST(NULL AS NUMBER)												sourceoracledbRecordCount
            ,			CAST(NULL AS NUMBER)												CDBRecordCount
            ,			CAST(NULL AS NUMBER)												targetoracledbrecordcount
            ,			CAST(NULL AS NUMBER)												sasDATASETRECORDCOUNT
            ,			CAST(NULL AS NUMBER)												RecordCountDifference
            ,			CAST(NULL AS TIMESTAMP)												InitiatedDateTime
            ,			CAST(NULL AS TIMESTAMP)												CompletedDateTime
            ,			CAST(NULL AS INTERVAL DAY TO SECOND)								CompletedDuration
            ,			CAST(NULL AS TIMESTAMP)												ErrorDateTime
            ,           CAST(NULL AS NUMBER)                                                LOG_SK
            FROM		ar_controltable
        ) newdata
        ON (olddata.controltable_sk=newdata.controltable_sk)
            when MATCHED then update set  olddata.reconidentifier=newdata.reconidentifier where 1=2
            when NOT MATCHED THEN
                INSERT 
                (
                controltable_sk,
                reconidentifier,
                projectidentifier,
                projectname,
                tableidentifier,
                sourceoracledbtablename,
                cdbtablename,
                targetoracledbtablename,
                sourceoracledbrecordcount,
                cdbrecordcount,
                targetoracledbrecordcount,
                sasDATASETRECORDCOUNT,
                sourceoracledbtargetoracledb_recordcountdifference,
                initiateddatetime,
                completeddatetime,
                completedduration,
                errordatetime,
                LOG_SK
                )
                    VALUES
                (
                newdata.controltable_sk,
                newdata.reconidentifier,
                newdata.projectidentifier,
                newdata.projectname,
                newdata.tableidentifier,
                newdata.sourceoracledbtablename,
                newdata.cdbtablename,
                newdata.targetoracledbtablename,
                newdata.sourceoracledbrecordcount,
                newdata.cdbrecordcount,
                newdata.targetoracledbrecordcount,
                newdata.sasDATASETRECORDCOUNT,
                newdata.recordcountdifference,
                newdata.initiateddatetime,
                newdata.completeddatetime,
                newdata.completedduration,
                newdata.errordatetime,
                newdata.LOG_SK
                );
        COMMIT;
        
          END IF;
DBMS_OUTPUT.PUT_LINE('Loaded Metadata to load the results');
 END IF; --end if 
 


/*
EXCEPTION 
		WHEN others THEN
        UPDATE AR_execution_log
        SET 
        execution_error =SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000)
        ,comments='reccon engine execution failed ,please investigate through corresponding procedure '
        WHERE PROCEDURE_NME='PROC_GENERATEMETDAFORRECON' AND LOG_SK=PARAM_LOG_SK;
*/
END;
/
--set serveroutput on;
--EXEC proc_generatemetdaforrecon('Y','Y','Y','Y','Y');
--EXIT;


