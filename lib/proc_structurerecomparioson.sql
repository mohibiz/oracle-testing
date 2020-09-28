CREATE OR REPLACE PROCEDURE proc_structurecomparison(executetargetoracledbrecon VARCHAR2 Default 'N' ,executesasrecon VARCHAR2 Default 'N' ,PARAM_LOG_SK NUMBER DEFAULT -1)
AS 
/*To Do's */
--Write nested procedure to remove repeative code so the one procedure could be called to generated recon views for sourceoracledb and cdb.
--Don't create temp table if already exists

 CURSOR tableCursor IS
	SELECT		TL.controltable_sk ,  TL.reconidentifier  ,TL.projectidentifier  ,TL.projectname,TL.TableIdentifier, TL.sourceoracledbSchema, TL.sourceoracledbTableName
	,			TL.targetoracledbTableName, TL.TableKeys 
    ,TL.DATAVAR , TL.Datenotin, TL.DateGreaterThan, TL.DateLessThan 
	FROM		AR_ControlTable TL	 											
	;
  i_owner    VARCHAR2(30);
  i_object_type   VARCHAR2(30) ;
  i_object_name    VARCHAR2(30) ;
  object_exist  NUMBER;
    v_sourceoracledb_targetoracledb_column_mismatch AR_STRUCTURERESULTS.sourceoracledb_targetoracledb_column_mismatch%TYPE; 
	v_sourceoracledb_targetoracledb_datetype_mismatch AR_STRUCTURERESULTS.sourceoracledb_targetoracledb_datetype_mismatch%TYPE;
    v_sourceoracledb_targetoracledb_datalength_mismatch AR_STRUCTURERESULTS.sourceoracledb_targetoracledb_datalength_mismatch%TYPE;
	v_sourceoracledb_targetoracledb_data_prec_mismatch AR_STRUCTURERESULTS.sourceoracledb_targetoracledb_data_prec_mismatch%TYPE;
	v_cdb_targetoracledb_column_mismatch AR_STRUCTURERESULTS.cdb_targetoracledb_column_mismatch%TYPE; 
	v_cdb_targetoracledb_datetype_mismatch AR_STRUCTURERESULTS.cdb_targetoracledb_datetype_mismatch%TYPE;
	v_cdb_targetoracledb_datalength_mismatch AR_STRUCTURERESULTS.cdb_targetoracledb_datalength_mismatch%TYPE;
	v_cdb_targetoracledb_data_prec_mismatch AR_STRUCTURERESULTS.cdb_targetoracledb_data_prec_mismatch%TYPE;
    v_sourceoracledb_sas_column_mismatch AR_STRUCTURERESULTS.sourceoracledb_sas_column_mismatch%TYPE;
	v_sourceoracledb_sas_datetype_mismatch AR_STRUCTURERESULTS.sourceoracledb_sas_datetype_mismatch%TYPE;
    
    targetoracledbusername VARCHAR2(30);   
     
    BEGIN
      SELECT user INTO targetoracledbusername FROM DUAL;

    FOR c IN tableCursor
	LOOP
		BEGIN
   
			UPDATE	AR_STRUCTURERESULTS
			SET		InitiatedDateTime	=	LOCALTIMESTAMP
			--WHERE	TableIdentifier		=	c.TableIdentifier;
              WHERE	controltable_sk		=	c.controltable_sk;  
         
         IF executetargetoracledbrecon ='Y'  THEN   
             -------------------------------------------sourceoracledb TO targetoracledb Table STRUCTURE RECON----------------------------------
			EXECUTE IMMEDIATE	'CREATE TABLE ART_sourceoracledbtargetoracledb_columname' || c.controltable_sk || ' AS '
			    || CHR(10) ||	    'SELECT MIN(TableSource) TableSource, sourceoracledbTableName, targetoracledbTableName, COLUMN_NAME'
				|| CHR(10) ||		'FROM AR_STRUCVAL_sourceoracledb '
                || CHR(10) ||		'WHERE column_name not like ''ADM%'' and COLUMN_NAME not like ''PARTITION%'' and COLUMN_NAME!=''EXTARCHIVEIND'' and COLUMN_NAME!=''NGCM_ITERATION'''
                || CHR(10) ||		'and controltable_sk='||c.controltable_sk
                || CHR(10) ||        'GROUP BY  sourceoracledbTableName, targetoracledbTableName, COLUMN_NAME HAVING    COUNT(*) = 1  ORDER BY  sourceoracledbTableName, targetoracledbTableName, COLUMN_NAME'
            ;
            EXECUTE IMMEDIATE'CREATE TABLE ART_sourceoracledbtargetoracledb_datatype' || c.controltable_sk || ' AS'
				|| CHR(10) ||		'SELECT sourceoracledb.sourceoracledbTableName,sourceoracledb.targetoracledbTableName, sourceoracledb.COLUMN_NAME,sourceoracledb.DATA_TYPE  sourceoracledb_DATA_TYPE,  targetoracledb.DATA_TYPE targetoracledb_DATA_TYPE'
				|| CHR(10) ||		'FROM AR_STRUCVAL_sourceoracledb sourceoracledb'
                || CHR(10) ||		'INNER JOIN  AR_STRUCVAL_sourceoracledb  targetoracledb ON sourceoracledb.sourceoracledbTableName  = targetoracledb.sourceoracledbTableName AND   sourceoracledb.targetoracledbTableName=targetoracledb.targetoracledbTableName AND sourceoracledb.COLUMN_NAME = targetoracledb.COLUMN_NAME'
                || CHR(10) ||		'WHERE       sourceoracledb.TableSource =''sourceoracledb'' AND  targetoracledb.TableSource=''targetoracledb_sourceoracledb_SA1'' AND sourceoracledb.DATA_TYPE <> targetoracledb.DATA_TYPE'
                || CHR(10) ||		'and sourceoracledb.controltable_sk='||c.controltable_sk
                || CHR(10) ||        'ORDER BY  sourceoracledbTableName, targetoracledbTableName, COLUMN_NAME'
            ;
            
            EXECUTE IMMEDIATE 'CREATE TABLE ART_sourceoracledbtargetoracledb_datalength' || c.controltable_sk || ' AS'
				|| CHR(10) ||		'SELECT sourceoracledb.sourceoracledbTableName,sourceoracledb.targetoracledbTableName, sourceoracledb.COLUMN_NAME,sourceoracledb.DATA_LENGTH  sourceoracledb_DATA_LENGTH,  targetoracledb.DATA_LENGTH targetoracledb_DATA_LENGTH'
				|| CHR(10) ||		'FROM AR_STRUCVAL_sourceoracledb sourceoracledb'
                || CHR(10) ||		'INNER JOIN  AR_STRUCVAL_sourceoracledb  targetoracledb ON sourceoracledb.sourceoracledbTableName  = targetoracledb.sourceoracledbTableName AND   sourceoracledb.targetoracledbTableName=targetoracledb.targetoracledbTableName AND sourceoracledb.COLUMN_NAME = targetoracledb.COLUMN_NAME'
                || CHR(10) ||		'WHERE       sourceoracledb.TableSource =''sourceoracledb'' AND  targetoracledb.TableSource=''targetoracledb_sourceoracledb_SA1'' AND sourceoracledb.DATA_LENGTH <> targetoracledb.DATA_LENGTH'
                || CHR(10) ||		'and sourceoracledb.controltable_sk='||c.controltable_sk
                || CHR(10) ||        'ORDER BY  sourceoracledbTableName, targetoracledbTableName, COLUMN_NAME'
            ;
            EXECUTE IMMEDIATE 'CREATE TABLE ART_sourceoracledbtargetoracledb_dataprec' || c.controltable_sk || ' AS'
				|| CHR(10) ||		'SELECT sourceoracledb.sourceoracledbTableName,sourceoracledb.targetoracledbTableName, sourceoracledb.COLUMN_NAME,sourceoracledb.DATA_PRECISION  sourceoracledb_DATA_PRECISION,  targetoracledb.DATA_PRECISION targetoracledb_DATA_PRECISION'
				|| CHR(10) ||		'FROM AR_STRUCVAL_sourceoracledb sourceoracledb'
                || CHR(10) ||		'INNER JOIN  AR_STRUCVAL_sourceoracledb  targetoracledb ON sourceoracledb.sourceoracledbTableName  = targetoracledb.sourceoracledbTableName AND   sourceoracledb.targetoracledbTableName=targetoracledb.targetoracledbTableName AND sourceoracledb.COLUMN_NAME = targetoracledb.COLUMN_NAME'
                || CHR(10) ||		'WHERE       sourceoracledb.TableSource =''sourceoracledb'' AND  targetoracledb.TableSource=''targetoracledb_sourceoracledb_SA1'' AND sourceoracledb.DATA_PRECISION <> targetoracledb.DATA_PRECISION'
                || CHR(10) ||		'and sourceoracledb.controltable_sk='||c.controltable_sk
                || CHR(10) ||        'ORDER BY  sourceoracledbTableName, targetoracledbTableName, COLUMN_NAME'
            ;
            -------------------------------------------CDB TO targetoracledb Table STRUCTURE RECON----------------------------------
            EXECUTE IMMEDIATE	'CREATE TABLE ART_cdbtargetoracledb_columname' || c.controltable_sk || ' AS '
				--|| CHR(10) ||		'SELECT MIN('''||c.tablesource||''') tablesource ,''' || c.sourceoracledbTableName ||''','''|| c.targetoracledbTableName||'''column_name'''
                || CHR(10) ||	    'SELECT MIN(TableSource) TableSource, CDBTableName, targetoracledbTableName, COLUMN_NAME'
				|| CHR(10) ||		'FROM AR_STRUCVAL_CDB '
                || CHR(10) ||		'WHERE column_name not like ''ADM%'' and COLUMN_NAME not like ''PARTITION%'' and COLUMN_NAME!=''EXTARCHIVEIND'' and COLUMN_NAME!=''NGCM_ITERATION'''
                || CHR(10) ||		'and controltable_sk='||c.controltable_sk
                || CHR(10) ||        'GROUP BY  CDBTableName , targetoracledbTableName, COLUMN_NAME HAVING    COUNT(*) = 1  ORDER BY  CDBTableName, targetoracledbTableName, COLUMN_NAME'
            ;
            EXECUTE IMMEDIATE 'CREATE TABLE ART_cdbtargetoracledb_datatype' || c.controltable_sk || ' AS'
				|| CHR(10) ||		'SELECT CDB.CDBTableName,CDB.targetoracledbTableName, CDB.COLUMN_NAME,CDB.DATA_TYPE  CDB_DATA_TYPE,  targetoracledb.DATA_TYPE targetoracledb_DATA_TYPE'
				|| CHR(10) ||		'FROM AR_STRUCVAL_CDB CDB'
                || CHR(10) ||		'INNER JOIN  AR_STRUCVAL_CDB  targetoracledb ON CDB.CDBTableName  = targetoracledb.CDBTableName AND   CDB.targetoracledbTableName=targetoracledb.targetoracledbTableName AND CDB.COLUMN_NAME = targetoracledb.COLUMN_NAME'
                || CHR(10) ||		'WHERE       CDB.TableSource =''sourceoracledb'' AND  targetoracledb.TableSource=''targetoracledb_sourceoracledb_SA1'' AND CDB.DATA_TYPE <> targetoracledb.DATA_TYPE'
                || CHR(10) ||		'and CDB.controltable_sk='||c.controltable_sk
                || CHR(10) ||        'ORDER BY  CDBTableName, targetoracledbTableName, COLUMN_NAME'
            ;
            
            EXECUTE IMMEDIATE 'CREATE TABLE ART_cdbtargetoracledb_datalength' || c.controltable_sk || ' AS'
				|| CHR(10) ||		'SELECT CDB.CDBTableName,CDB.targetoracledbTableName, CDB.COLUMN_NAME,CDB.DATA_LENGTH  CDB_DATA_LENGTH,  targetoracledb.DATA_LENGTH targetoracledb_DATA_LENGTH'
				|| CHR(10) ||		'FROM AR_STRUCVAL_CDB CDB'
                || CHR(10) ||		'INNER JOIN  AR_STRUCVAL_CDB  targetoracledb ON CDB.CDBTableName  = targetoracledb.CDBTableName AND   CDB.targetoracledbTableName=targetoracledb.targetoracledbTableName AND CDB.COLUMN_NAME = targetoracledb.COLUMN_NAME'
                || CHR(10) ||		'WHERE       CDB.TableSource =''CDB'' AND  targetoracledb.TableSource=''targetoracledb_sourceoracledb_SA1'' AND CDB.DATA_LENGTH <> targetoracledb.DATA_LENGTH'
                || CHR(10) ||		'and CDB.controltable_sk='||c.controltable_sk
                || CHR(10) ||        'ORDER BY  CDBTableName, targetoracledbTableName, COLUMN_NAME'
            ;
            EXECUTE IMMEDIATE   'CREATE TABLE ART_cdbtargetoracledb_dataprec' || c.controltable_sk || ' AS'
				|| CHR(10) ||		'SELECT CDB.CDBTableName,CDB.targetoracledbTableName, CDB.COLUMN_NAME,CDB.DATA_PRECISION  CDB_DATA_PRECISION,  targetoracledb.DATA_PRECISION targetoracledb_DATA_PRECISION'
				|| CHR(10) ||		'FROM AR_STRUCVAL_CDB CDB'
                || CHR(10) ||		'INNER JOIN  AR_STRUCVAL_CDB  targetoracledb ON CDB.CDBTableName  = targetoracledb.CDBTableName AND   CDB.targetoracledbTableName=targetoracledb.targetoracledbTableName AND CDB.COLUMN_NAME = targetoracledb.COLUMN_NAME'
                || CHR(10) ||		'WHERE       CDB.TableSource =''CDB'' AND  targetoracledb.TableSource=''targetoracledb_sourceoracledb_SA1'' AND CDB.DATA_PRECISION <> targetoracledb.DATA_PRECISION'
                || CHR(10) ||		'and CDB.controltable_sk='||c.controltable_sk
                || CHR(10) ||        'ORDER BY  CDBTableName, targetoracledbTableName, COLUMN_NAME'
            ;
           END IF; 
           
           IF executesasrecon='Y' THEN
       -------------------------------------------sourceoracledb TO sas Table STRUCTURE RECON----------------------------------
             EXECUTE IMMEDIATE	'CREATE TABLE ART_sourceoracledbsas_columname' || c.controltable_sk || ' AS '
				--|| CHR(10) ||		'SELECT MIN('''||c.tablesource||''') tablesource ,''' || c.sourceoracledbTableName ||''','''|| c.targetoracledbTableName||'''column_name'''
                || CHR(10) ||	    'SELECT MIN(TableSource) TableSource, sourceoracledbTABLENAME, sasDATASETNAME, COLUMN_NAME'
				|| CHR(10) ||		'FROM AR_STRUCVAL_sourceoracledb '
                || CHR(10) ||		'WHERE column_name not like ''ADM%'' and COLUMN_NAME not like ''PARTITION%'' and COLUMN_NAME!=''EXTARCHIVEIND'' and COLUMN_NAME!=''NGCM_ITERATION'''
                || CHR(10) ||		'and controltable_sk='||c.controltable_sk
                || CHR(10) ||        'GROUP BY  sourceoracledbTABLENAME , sasDATASETNAME, COLUMN_NAME HAVING    COUNT(*) = 1  ORDER BY  sourceoracledbTABLENAME, sasDATASETNAME, COLUMN_NAME'
            ;
              EXECUTE IMMEDIATE	'CREATE TABLE ART_sourceoracledbsas_datatype' || c.controltable_sk || ' AS'
				|| CHR(10) ||		'SELECT sas.sourceoracledbTABLENAME,sas.sasDATASETNAME, sas.COLUMN_NAME,sourceoracledb.DATA_TYPE  sourceoracledb_DATA_TYPE,  sas.DATA_TYPE sas_DATA_TYPE'
				|| CHR(10) ||		'FROM AR_STRUCVAL_sourceoracledb sas'
                || CHR(10) ||		'INNER JOIN  AR_STRUCVAL_sourceoracledb  sourceoracledb ON sourceoracledb.sourceoracledbTABLENAME  = sas.sourceoracledbTABLENAME AND   sourceoracledb.sasDATASETNAME=sas.sasDATASETNAME AND sourceoracledb.COLUMN_NAME = sas.COLUMN_NAME'
                || CHR(10) ||		'WHERE       sourceoracledb.TableSource =''sas'' AND  sas.TableSource='''||targetoracledbusername  ||'''  AND sourceoracledb.DATA_TYPE <> sas.DATA_TYPE' --replace the tablesource from input parameter from system config files
                || CHR(10) ||		'and sourceoracledb.controltable_sk='||c.controltable_sk
                || CHR(10) ||        'ORDER BY  sourceoracledbTABLENAME, sasDATASETNAME, COLUMN_NAME'
            ;
        END IF;
            ---------------------------------------------Updating Results counts----------------------------------------------------------
            --Check to see if temporary tables has been created or not.If not then set the value to null

            
            i_object_name :='ART_sourceoracledbtargetoracledb_COLUMNAME' || c.controltable_sk;
           v_sourceoracledb_targetoracledb_column_mismatch:= get_table_rowcount(i_object_name,targetoracledbusername);
            i_object_name :='ART_sourceoracledbtargetoracledb_DATATYPE' || c.controltable_sk;
           v_sourceoracledb_targetoracledb_datetype_mismatch:=get_table_rowcount(i_object_name,targetoracledbusername);
            i_object_name :='ART_sourceoracledbtargetoracledb_DATALENGTH' || c.controltable_sk;
           v_sourceoracledb_targetoracledb_datalength_mismatch:=get_table_rowcount(i_object_name,targetoracledbusername);
           i_object_name :='ART_sourceoracledbtargetoracledb_DATAPREC' || c.controltable_sk;
           v_sourceoracledb_targetoracledb_data_prec_mismatch:=get_table_rowcount(i_object_name,targetoracledbusername);
           
           i_object_name :='ART_CDBtargetoracledb_COLUMNAME' || c.controltable_sk;
           v_cdb_targetoracledb_column_mismatch:= get_table_rowcount(i_object_name,targetoracledbusername);
            i_object_name :='ART_CDBtargetoracledb_DATATYPE' || c.controltable_sk;
           v_cdb_targetoracledb_datetype_mismatch:=get_table_rowcount(i_object_name,targetoracledbusername);
            i_object_name :='ART_CDBtargetoracledb_DATALENGTH' || c.controltable_sk;
           v_cdb_targetoracledb_datalength_mismatch:=get_table_rowcount(i_object_name,targetoracledbusername);
           i_object_name :='ART_CDBtargetoracledb_DATAPREC' || c.controltable_sk;
           v_cdb_targetoracledb_data_prec_mismatch:=get_table_rowcount(i_object_name,targetoracledbusername);
            i_object_name :='ART_sourceoracledbsas_COLUMNAME' || c.controltable_sk;
           v_sourceoracledb_sas_column_mismatch:=get_table_rowcount(i_object_name,targetoracledbusername);
           i_object_name :='ART_sourceoracledbsas_DATATYPE' || c.controltable_sk;
           v_sourceoracledb_sas_datetype_mismatch:=get_table_rowcount(i_object_name,targetoracledbusername);
       
             
			 EXECUTE IMMEDIATE	
            'UPDATE AR_STRUCTURERESULTS'
				|| CHR(10) ||	'SET sourceoracledb_targetoracledb_column_mismatch = ''' || v_sourceoracledb_targetoracledb_column_mismatch || ''''
				|| CHR(10) ||	', sourceoracledb_targetoracledb_datetype_mismatch = ''' || v_sourceoracledb_targetoracledb_column_mismatch || ''''
                || CHR(10) ||	', sourceoracledb_targetoracledb_datalength_mismatch = ''' || v_sourceoracledb_targetoracledb_column_mismatch || ''''
                || CHR(10) ||	', sourceoracledb_targetoracledb_data_prec_mismatch = ''' || v_sourceoracledb_targetoracledb_column_mismatch || ''''
                || CHR(10) ||	', cdb_targetoracledb_column_mismatch = ''' || v_sourceoracledb_targetoracledb_column_mismatch || ''''
                || CHR(10) ||	', cdb_targetoracledb_datetype_mismatch = ''' || v_sourceoracledb_targetoracledb_column_mismatch || ''''
                || CHR(10) ||	', cdb_targetoracledb_datalength_mismatch = ''' || v_sourceoracledb_targetoracledb_column_mismatch || ''''
                || CHR(10) ||	', cdb_targetoracledb_data_prec_mismatch = ''' || v_sourceoracledb_targetoracledb_column_mismatch || ''''
                || CHR(10) ||	', sourceoracledb_sas_column_mismatch = ''' || v_sourceoracledb_sas_column_mismatch || ''''
                || CHR(10) ||	', sourceoracledb_sas_datetype_mismatch = ''' || v_sourceoracledb_sas_datetype_mismatch || ''''
				|| CHR(10) ||	', CompletedDateTime = LOCALTIMESTAMP'
				|| CHR(10) ||	', CompletedDuration = LOCALTIMESTAMP - InitiatedDateTime'
                || CHR(10) ||	', LOG_SK = ''' || PARAM_LOG_SK || ''''
				|| CHR(10) ||	'WHERE controltable_sk = ''' || c.controltable_sk || ''''
			;

COMMIT;
		EXCEPTION
		WHEN others THEN
            
            INSERT INTO AR_STRUCTUREERRORS(controltable_sk,reconidentifier,projectidentifier,projectname,TableIdentifier,TableSource, TableName, Message,LOG_SK)
			VALUES(c.controltable_sk,c.reconidentifier,c.projectidentifier,c.projectname,c.TableIdentifier,'sourceoracledb to targetoracledb and sas', c.sourceoracledbTableName, SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000),PARAM_LOG_SK);
			
    END;
           
	END LOOP;
    
    COMMIT;
END;

/
--set serveroutput on;
--EXEC PROC_STRUCTURECOMPARISON('Y','Y',100);
