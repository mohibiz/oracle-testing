CREATE OR REPLACE PROCEDURE proc_generatemetdaforrecon(exestructurecrecon VARCHAR2 Default 'N' ,exerowcountcrecon VARCHAR2 Default 'N' , exeallcolumncrecon VARCHAR2 Default 'N' ,executetargetoracledbrecon VARCHAR2 Default 'N' ,executesasrecon VARCHAR2 Default 'N' ,PARAM_LOG_SK NUMBER DEFAULT -1 )
IS 
-------------------------
--If one table fails it fails the whole procedure
------------------------
    targetoracledbusername VARCHAR2(30);   
BEGIN 
    SELECT user INTO targetoracledbusername FROM DUAL;
--------------------------------------
--------------------------------------
    
IF ( executetargetoracledbrecon='Y' AND exeallcolumncrecon='Y' )
THEN
--Prepare mutualcolumn list so data could be compared for mutual columns , this list is to be used for preparing comma seperated values 

INSERT INTO ar_mutualcolumns 
WITH TransformationRules AS
(
				SELECT 'STARTDATEACTIVE' COLUMN_NAME, 2 TransformationID FROM DUAL
	UNION ALL	SELECT 'START_DATE_ACTIVE', 2 FROM DUAL
	UNION ALL	SELECT 'EFFECTIVEFROM', 2 FROM DUAL
	UNION ALL	SELECT 'EFFECTIVE_FROM', 2 FROM DUAL
	UNION ALL	SELECT 'EFFECTIVEFROMDATE', 2 FROM DUAL
	UNION ALL	SELECT 'EFFECTIVE_FROM_DATE', 2 FROM DUAL
	UNION ALL	SELECT 'ENDDATEACTIVE', 3 FROM DUAL
	UNION ALL	SELECT 'END_DATE_ACTIVE', 3 FROM DUAL
	UNION ALL	SELECT 'EFFECTIVETO', 3 FROM DUAL
	UNION ALL	SELECT 'EFFECTIVE_TO', 3 FROM DUAL
	UNION ALL	SELECT 'EFFECTIVETODATE', 3 FROM DUAL
	UNION ALL	SELECT 'EFFECTIVE_TO_DATE', 3 FROM DUAL
	UNION ALL	SELECT 'LASTUPDATEDATE', 4 FROM DUAL
)
, Transformation_sourceoracledb AS
(
				SELECT 1 TransformationID, 'LTRIM(RTRIM([X])) [Y]' sourceoracledbTransformationSQL FROM DUAL
	UNION ALL	SELECT 2, 'COALESCE(NULLIF([Y], TO_DATE(''01/01/1753'', ''dd/mm/yyyy'')), TO_DATE(''01/01/1000'', ''dd/mm/yyyy'')) [Y]' FROM DUAL
	UNION ALL	SELECT 3, 'COALESCE(NULLIF([Y], TO_DATE(''01/01/1753'', ''dd/mm/yyyy'')), TO_DATE(''31/12/8999'', ''dd/mm/yyyy'')) [Y]' FROM DUAL
	UNION ALL	SELECT 4, '[X]' FROM DUAL
	UNION ALL	SELECT 5, 'COALESCE(NULLIF([Y], TO_DATE(''01/01/1753'', ''dd/mm/yyyy'')), TO_DATE(''29/12/8999'', ''dd/mm/yyyy'')) [Y]' FROM DUAL
   -- UNION ALL	SELECT 9, 'NVL([X],''NA'') [Y]' FROM DUAL
    UNION ALL	SELECT 9, '[X] [Y]' FROM DUAL
)
, Transformation_targetoracledb AS
(
				SELECT 1 TransformationID, 'LTRIM(RTRIM([X])) [Y]' targetoracledbTransformationSQL FROM DUAL
  --  UNION ALL	SELECT 9, 'NVL([X],''NA'')' FROM DUAL
  UNION ALL	SELECT 9, '[X] [Y]'  FROM DUAL
)
,targetoracledbColumns AS 
    (SELECT OWNER, TABLE_NAME, OWNER||'.'||TABLE_NAME QULAIFIED_TABLE_NAME ,COLUMN_NAME, OWNER||'.'||TABLE_NAME||'.'||COLUMN_NAME QUALIFIED_COLUMN_NAME,DATA_TYPE,COLUMN_ID ,TABLE_NAME||'.'||COLUMN_NAME TABLE_COLUMN_NAME FROM SYS.ALL_TAB_COLUMNS)
,sourceoracledbColumns AS     
    (SELECT OWNER, TABLE_NAME, OWNER||'.'||TABLE_NAME QULAIFIED_TABLE_NAME ,COLUMN_NAME, OWNER||'.'||TABLE_NAME||'.'||COLUMN_NAME QUALIFIED_COLUMN_NAME,DATA_TYPE,COLUMN_ID,TABLE_NAME||'.'||COLUMN_NAME TABLE_COLUMN_NAME   FROM SYS.ALL_TAB_COLUMNS@DBLinksourceoracledb)
, MutualColumns AS 
  (
	SELECT		/*+ PARALLEL(t,4) */
                TL.controltable_sk
    ,           TL.reconidentifier
    ,           TL.projectidentifier
    ,           TL.projectname
	,			TL.TableIdentifier
	,			targetoracledb.COLUMN_NAME 
    ,           targetoracledb.DATA_TYPE targetoracledb_DATA_TYPE
    --,           COALESCE(REPLACE(REPLACE(T2.targetoracledbTransformationSQL, '[X]', sourceoracledb.TABLE_COLUMN_NAME),'[Y]',sourceoracledb.COLUMN_NAME||'_LOB' ), sourceoracledb.TABLE_COLUMN_NAME||' '||sourceoracledb.COLUMN_NAME||'_LOB'  )		   TRANSFORMED_CLOUMN
    ,           COALESCE(REPLACE(REPLACE(T2.targetoracledbTransformationSQL, '[X]', sourceoracledb.TABLE_COLUMN_NAME),'[Y]','' ), sourceoracledb.TABLE_COLUMN_NAME||' '||sourceoracledb.COLUMN_NAME)		   TRANSFORMED_CLOUMN
    ,           targetoracledb.OWNER                                                                                  targetoracledb_SCHEMA
    ,           COALESCE(REPLACE(REPLACE(T2.targetoracledbTransformationSQL, '[X]', targetoracledb.QUALIFIED_COLUMN_NAME),'[Y]',targetoracledb.COLUMN_NAME||'_targetoracledb'), targetoracledb.QUALIFIED_COLUMN_NAME||' '||targetoracledb.COLUMN_NAME||'_targetoracledb')		   targetoracledb_COLUMN_ALIAS
    ,           COALESCE(REPLACE(REPLACE(T2.targetoracledbTransformationSQL, '[X]',targetoracledb.QUALIFIED_COLUMN_NAME),'[Y]',''), targetoracledb.QUALIFIED_COLUMN_NAME)		   targetoracledb_COLUMN
    ,           TL.sourceoracledbSchema                                                                               sourceoracledb_SCHEMA
   	,			COALESCE(REPLACE(REPLACE(T1.sourceoracledbTransformationSQL, '[X]', sourceoracledb.QUALIFIED_COLUMN_NAME),'[Y]',sourceoracledb.COLUMN_NAME||'_sourceoracledb'), sourceoracledb.QUALIFIED_COLUMN_NAME||' '||sourceoracledb.COLUMN_NAME||'_sourceoracledb')		    sourceoracledb_COLUMN_ALIAS
    ,			COALESCE(REPLACE(REPLACE(T1.sourceoracledbTransformationSQL, '[X]', sourceoracledb.COLUMN_NAME),'[Y]',sourceoracledb.COLUMN_NAME), sourceoracledb.COLUMN_NAME)		    sourceoracledb_COLUMN
    ,           sourceoracledb.DATA_TYPE sourceoracledb_DATA_TYPE
	,			NULL sas_schema  
	,			NULL sas_column 
	,			NULL sas_data_type 
	,			ROW_NUMBER() OVER(PARTITION BY TL.controltable_sk,TL.TableIdentifier ORDER BY sourceoracledb.COLUMN_ID, targetoracledb.COLUMN_ID)	ColumnOrder
	FROM		targetoracledbColumns												targetoracledb
	INNER JOIN	sourceoracledbColumns                                      		sourceoracledb
	ON			targetoracledb.COLUMN_NAME											=			sourceoracledb.COLUMN_NAME
	INNER JOIN	ar_ControlTable															TL
	ON			sourceoracledb.TABLE_NAME											=			TL.sourceoracledbTableName
	AND			sourceoracledb.OWNER												=			TL.sourceoracledbSchema
	AND			targetoracledb.TABLE_NAME											=			TL.targetoracledbTABLENAME
	AND			targetoracledb.OWNER												=			'sourceoracledb_SA1'
    AND         TL.targetoracledbRECON_IND='Y'
   LEFT JOIN	TransformationRules													TR
	ON			targetoracledb.COLUMN_NAME											=			TR.COLUMN_NAME
	AND		(	targetoracledb.DATA_TYPE											=			'DATE'
	OR			targetoracledb.DATA_TYPE											LIKE		'TIMESTAMP%')
	LEFT JOIN	Transformation_sourceoracledb														T1
	ON			COALESCE(TR.TransformationID, CASE
					WHEN	targetoracledb.DATA_TYPE IN ('CHAR', 'VARCHAR', 'VARCHAR2') THEN 1
                    WHEN targetoracledb.DATA_TYPE = 'CLOB' THEN 9
					WHEN	targetoracledb.DATA_TYPE = 'DATE'
					OR		targetoracledb.DATA_TYPE LIKE 'TIMESTAMP%' THEN CASE
						WHEN	targetoracledb.COLUMN_NAME LIKE '%STARTDATE'
						OR		targetoracledb.COLUMN_NAME LIKE '%START_DATE'	THEN 2
						WHEN	targetoracledb.COLUMN_NAME LIKE '%ENDDATE'
						OR		targetoracledb.COLUMN_NAME LIKE '%END_DATE'	THEN 3
                    											ELSE 5	END END)	=			T1.TransformationID
    LEFT JOIN Transformation_targetoracledb                                                        T2
    ON COALESCE(TR.TransformationID, CASE
					WHEN	targetoracledb.DATA_TYPE IN ('CHAR', 'VARCHAR', 'VARCHAR2') THEN 1
                    WHEN targetoracledb.DATA_TYPE = 'CLOB' THEN 9
					 END)	=			T2.TransformationID
)
, EXCLUDE_COLUMLIST AS (SELECT 
       t.controltable_sk,
        t.TABLEIDENTIFIER,
       v.column_value AS EXCLUDECOLUMN
FROM   ar_controltable t,
       TABLE( split_String( t.EXCLUDCOLUMNLIST ) ) v)
SELECT * FROM mutualcolumns T1 WHERE NOT EXISTS (SELECT 1 FROM EXCLUDE_COLUMLIST T2 WHERE T1.controltable_sk=t2.controltable_sk AND T1.COLUMN_NAME=T2.EXCLUDECOLUMN);
COMMIT;

DBMS_OUTPUT.PUT_LINE('Loaded metdata for common purposes');

--Prepare mutual column list for LOB Columns 

INSERT INTO ar_MutualColumnLobs
WITH TransformationRules AS
(
				SELECT 'CLOB' DATA_TYPE, 1 TransformationID FROM DUAL
	
)
, Transformation_Lob AS
(
				SELECT 1 TransformationID, 'dbms_lob.compare(NVL([X],''NA''),NVL([Y],''NA'')) [Z]' TransformationSQL FROM DUAL

)
,  MutualColumnLob AS
(
	SELECT		
                controltable_sk
    ,           reconidentifier
    ,           projectidentifier
    ,           projectname
    ,           TableIdentifier
	,			COLUMN_NAME
    ,           TRANSFORMED_CLOUMN
    ,           targetoracledb_SCHEMA
    ,           sourceoracledb_SCHEMA
    ,           targetoracledb_COLUMN
    ,           targetoracledb_COLUMN_ALIAS
	,			sourceoracledb_COLUMN
    ,			sourceoracledb_COLUMN_ALIAS
	,			CASE WHEN ColumnOrder <> 1
				THEN ', ' ELSE '' END												COMMA
	,			ColumnOrder
    ,           targetoracledb_DATA_TYPE
    ,           sourceoracledb_DATA_TYPE
    , COALESCE(REPLACE(REPLACE(REPLACE(T.TransformationSQL, '[X]', MC.targetoracledb_COLUMN),'[Y]',MC.TRANSFORMED_CLOUMN),'[Z]','COMPARE_'||MC.COLUMN_NAME ),'')		   TRANSFORMED_LOB_CLOUMN	
    , DECODE(MC.sourceoracledb_DATA_TYPE , 'CLOB', 'COMPARE_'||MC.COLUMN_NAME,null )   TRANSFORMED_LOB_CLOUMN_ALIAS	
    FROM		ar_MutualColumns                                                       MC
     LEFT JOIN	TransformationRules													TR
	ON			MC.sourceoracledb_DATA_TYPE										=			TR.DATA_TYPE
	    LEFT JOIN Transformation_Lob                                                        T
    ON COALESCE(TR.TransformationID, CASE
					WHEN MC.sourceoracledb_DATA_TYPE = 'CLOB' THEN 1 ELSE 99
					 END)	=			T.TransformationID
)SELECT * FROM MutualColumnLob;
COMMIT;

-- Prepare comma seperated list for mutual lob columns 
EXECUTE IMMEDIATE 'TRUNCATE TABLE ar_mutualcolumnloblist';
INSERT INTO ar_mutualcolumnloblist
WITH  MutualColumnLobListPrep AS
(
	SELECT		controltable_sk
    ,           reconidentifier
    ,           projectidentifier
    ,           projectname
    ,           TableIdentifier
	,			COLUMN_NAME
    ,           TRANSFORMED_CLOUMN
    ,           targetoracledb_SCHEMA
    ,           sourceoracledb_SCHEMA
    ,           targetoracledb_COLUMN
    ,           targetoracledb_COLUMN_ALIAS
	,			sourceoracledb_COLUMN
    ,			sourceoracledb_COLUMN_ALIAS
	,			CASE WHEN ColumnOrder <> 1  --targetoracledb_DATA_TYPE ='CLOB'
				THEN ', ' ELSE '' END												COMMA
   	,			CASE WHEN  ColumnOrder <> 1  AND sourceoracledb_DATA_TYPE = 'CLOB'  
				THEN ', ' ELSE '' END												LOBCOMMA
    ,			ColumnOrder
    ,           TRANSFORMED_LOB_CLOUMN
    ,           targetoracledb_DATA_TYPE
    ,           sourceoracledb_DATA_TYPE
    ,           TRANSFORMED_LOB_CLOUMN_ALIAS
    ,			CASE WHEN  ColumnOrder <> 1  AND sourceoracledb_DATA_TYPE = 'CLOB'  
				THEN '<>0 OR ' ELSE '' END										LOBRECONCONDTION1
    
	FROM		ar_MutualColumnLobs
)
SELECT		    controltable_sk
    ,           reconidentifier
    ,           projectidentifier
    ,           projectname
,TableIdentifier
,			dbms_xmlgen.convert(xmlagg(XMLELEMENT(E, COMMA||COLUMN_NAME) ORDER BY ColumnOrder).extract('//text()').getclobval() ,1)	MutualColumnList
,           dbms_xmlgen.convert(xmlagg(XMLELEMENT(E, COMMA||TRANSFORMED_CLOUMN) ORDER BY ColumnOrder).extract('//text()').getclobval() ,1) TransformedColumnList
,			dbms_xmlgen.convert(xmlagg(XMLELEMENT(E, COMMA||sourceoracledb_COLUMN_ALIAS) ORDER BY ColumnOrder).extract('//text()').getclobval() ,1)	sourceoracledbColumnAliasList
,			dbms_xmlgen.convert(xmlagg(XMLELEMENT(E, COMMA||targetoracledb_COLUMN_ALIAS) ORDER BY ColumnOrder).extract('//text()').getclobval() ,1)	targetoracledbColumnAliasList
,			dbms_xmlgen.convert(xmlagg(XMLELEMENT(E, COMMA||sourceoracledb_COLUMN) ORDER BY ColumnOrder).extract('//text()').getclobval() ,1)	sourceoracledbColumnList
,			dbms_xmlgen.convert(xmlagg(XMLELEMENT(E, COMMA||targetoracledb_COLUMN) ORDER BY ColumnOrder).extract('//text()').getclobval() ,1)	targetoracledbColumnList
,			dbms_xmlgen.convert(xmlagg(XMLELEMENT(E, LOBCOMMA||TRANSFORMED_LOB_CLOUMN) ORDER BY ColumnOrder).extract('//text()').getclobval() ,1)	CompareLOBColumnList
,			dbms_xmlgen.convert(xmlagg(XMLELEMENT(E, LOBRECONCONDTION1||TRANSFORMED_LOB_CLOUMN_ALIAS) ORDER BY ColumnOrder).extract('//text()').getclobval() ,1)||'<>0'	LOBReconCondition
FROM		MutualColumnLobListPrep
GROUP BY	controltable_sk,reconidentifier,projectidentifier,projectname,  TableIdentifier
;
COMMIT;
DBMS_OUTPUT.PUT_LINE('Loaded Metadata for targetoracledb all column comparison');
END IF;



  
  --------------INSERT metdata for table strcuture comparison---------------

IF executetargetoracledbrecon='Y' AND exestructurecrecon='Y'  THEN 

MERGE INTO AR_STRUCVAL_sourceoracledb olddata
USING(
WITH TableDetail AS
(
	SELECT		
                controltable_sk
    ,           reconidentifier
    ,           projectidentifier
    ,           projectname
    ,           TableIdentifier
    ,           'targetoracledb_sourceoracledb_SA1'														TableSource
	,			OWNER
	,			TL.sourceoracledbTableName														sourceoracledbTableName
	,			TL.targetoracledbTABLENAME												targetoracledbTableName
	,			COLUMN_NAME
	,			ATC.DATA_TYPE
	,			DATA_LENGTH
    
    
	,			DATA_PRECISION
	FROM		ALL_TAB_COLUMNS														ATC
	INNER JOIN	ar_controltable															TL
	ON			ATC.TABLE_NAME											=			TL.targetoracledbTABLENAME
	WHERE		ATC.OWNER												=			'sourceoracledb_SA1'
		UNION ALL
	SELECT		         
                controltable_sk
    ,           reconidentifier
    ,           projectidentifier
    ,           projectname
    ,           TableIdentifier
    ,           'sourceoracledb'																TableSource
	,			OWNER
	,			TL.sourceoracledbTableName														sourceoracledbTableName
	,			TL.targetoracledbTABLENAME												targetoracledbTableName
	,			COLUMN_NAME
	,			DATA_TYPE
	,			DATA_LENGTH
	,			DATA_PRECISION
	FROM		(SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION
					FROM ALL_TAB_COLUMNS@DBLinksourceoracledb)									ATC
	INNER JOIN	ar_controltable															TL
	ON			ATC.TABLE_NAME											=			TL.sourceoracledbTableName
	WHERE		ATC.OWNER												=			TL.sourceoracledbSchema
)

SELECT		     controltable_sk
    ,           reconidentifier
    ,           projectidentifier
    ,           projectname
    ,           TableIdentifier
    
   ,         MIN(TableSource)														TableSource
,			MIN(OWNER)																OWNER

,			sourceoracledbTableName
,			targetoracledbTableName
,			COLUMN_NAME
,			DATA_TYPE
,			DATA_LENGTH
,			DATA_PRECISION
FROM		TableDetail
GROUP BY	controltable_sk ,reconidentifier,projectidentifier,projectname , TableIdentifier,sourceoracledbTableName, targetoracledbTableName, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION
HAVING		COUNT(*) = 1
--ORDER BY	controltable_sk ,reconidentifier,projectidentifier,projectname , TableIdentifier,sourceoracledbTableName, targetoracledbTableName, COLUMN_NAME
) newdata
 ON (olddata.controltable_sk=newdata.controltable_sk)
    when MATCHED then update set  olddata.reconidentifier=newdata.reconidentifier where 1=2
    when NOT MATCHED THEN
        INSERT 
        (
            CONTROLTABLE_SK ,
            RECONIDENTIFIER  ,
            PROJECTIDENTIFIER ,
            PROJECTNAME  ,
            TABLEIDENTIFIER  ,
            TABLESOURCE , 
            OWNER , 
            sourceoracledbTABLENAME , 
            targetoracledbTABLENAME , 
            COLUMN_NAME , 
            DATA_TYPE , 
            DATA_LENGTH , 
            DATA_PRECISION 
        )
        VALUES
        (
            newdata.CONTROLTABLE_SK ,
            newdata.RECONIDENTIFIER  ,
            newdata.PROJECTIDENTIFIER ,
            newdata.PROJECTNAME  ,
            newdata.TABLEIDENTIFIER  ,
            newdata.TABLESOURCE , 
            newdata.OWNER , 
            newdata.sourceoracledbTABLENAME , 
            newdata.targetoracledbTABLENAME , 
            newdata.COLUMN_NAME , 
            newdata.DATA_TYPE , 
            newdata.DATA_LENGTH , 
            newdata.DATA_PRECISION 
        )
;
MERGE INTO AR_STRUCVAL_CDB  olddata
USING(
WITH TableDetail AS
(
	SELECT		
                controltable_sk
    ,           reconidentifier
    ,           projectidentifier
    ,           projectname
    ,           TableIdentifier
    ,           'targetoracledb_sourceoracledb_SA1'														TableSource
	,			OWNER
	,			TL.CDBTableName														CDBTableName
	,			TL.targetoracledbTABLENAME												targetoracledbTableName
	,			COLUMN_NAME
	,			ATC.DATA_TYPE
	,			DATA_LENGTH
	,			DATA_PRECISION
	FROM		ALL_TAB_COLUMNS														ATC
	INNER JOIN	ar_controltable															TL
	ON			ATC.TABLE_NAME											=			TL.targetoracledbTABLENAME
	WHERE		ATC.OWNER												=			'sourceoracledb_SA1'
	  UNION ALL
	SELECT		
                controltable_sk
    ,           reconidentifier
    ,           projectidentifier
    ,           projectname
    ,           TableIdentifier
    ,           'CDB'																TableSource
	,			OWNER
	,			TL.CDBTableName														CDBTableName
	,			TL.targetoracledbTABLENAME												targetoracledbTableName
	,			COLUMN_NAME
	,			DATA_TYPE
	,			DATA_LENGTH
	,			DATA_PRECISION
	FROM		(SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION
					FROM ALL_TAB_COLUMNS@DBLinkCDB)									ATC
	INNER JOIN	ar_controltable															TL
	ON			ATC.TABLE_NAME											=			TL.CDBTableName
	WHERE		ATC.OWNER												=			TL.CDBSchema
)

SELECT		

                controltable_sk
    ,           reconidentifier
    ,           projectidentifier
    ,           projectname
    ,           TableIdentifier
    ,       MIN(TableSource)														TableSource
,			MIN(OWNER)																OWNER
,			CDBTableName
,			targetoracledbTableName
,			COLUMN_NAME
,			DATA_TYPE
,			DATA_LENGTH
,			DATA_PRECISION
FROM		TableDetail
GROUP BY	controltable_sk ,reconidentifier,projectidentifier,projectname , TableIdentifier,CDBTableName, targetoracledbTableName, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION
HAVING		COUNT(*) = 1
--ORDER BY	CDBTableName, targetoracledbTableName, COLUMN_NAME
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
    tablesource,
    owner,
    cdbtablename,
    targetoracledbtablename,
    column_name,
    data_type,
    data_length,
    data_precision
        )
        VALUES
        (
            newdata.CONTROLTABLE_SK ,
            newdata.RECONIDENTIFIER  ,
            newdata.PROJECTIDENTIFIER ,
            newdata.PROJECTNAME  ,
            newdata.TABLEIDENTIFIER  ,
            newdata.TABLESOURCE , 
            newdata.OWNER , 
            newdata.cdbtablename , 
            newdata.targetoracledbTABLENAME , 
            newdata.COLUMN_NAME , 
            newdata.DATA_TYPE , 
            newdata.DATA_LENGTH , 
            newdata.DATA_PRECISION 
        )
;
COMMIT;
DBMS_OUTPUT.PUT_LINE('loaded data for targetoracledb structure comparsions');

END IF;


 --------------INSERT metdata for table strcuture comparison---------------

IF executesasrecon='Y' AND exestructurecrecon='Y'  THEN 
MERGE INTO AR_STRUCVAL_sourceoracledb olddata --ideally table name should be generic wrt to targetoracledb and sas
USING(
WITH TableDetail AS
(
	SELECT		
                controltable_sk
    ,           reconidentifier
    ,           projectidentifier
    ,           projectname
    ,           TableIdentifier
    ,           'sas'														TableSource
	,			OWNER
	,			TL.sourceoracledbTableName														sourceoracledbTableName
	,			TL.targetoracledbTABLENAME												targetoracledbTableName
    ,			TL.sasDATASETNAME												sasDATASETNAME
	,			COLUMN_NAME
	,			ATC.DATA_TYPE
	,			DATA_LENGTH
    
    
	,			DATA_PRECISION
	FROM		ALL_TAB_COLUMNS														ATC
	INNER JOIN	ar_controltable															TL
	ON			ATC.TABLE_NAME											=			TL.sasdatasetname
	WHERE		ATC.OWNER												=			targetoracledbusername
		UNION ALL
	SELECT		         
                controltable_sk
    ,           reconidentifier
    ,           projectidentifier
    ,           projectname
    ,           TableIdentifier
    ,           'sourceoracledb'																TableSource
	,			OWNER
	,			TL.sourceoracledbTableName														sourceoracledbTableName
	,			TL.targetoracledbTABLENAME												targetoracledbTableName
    ,			TL.sasDATASETNAME												sasDATASETNAME
	,			COLUMN_NAME
	,			DATA_TYPE
	,			DATA_LENGTH
	,			DATA_PRECISION
	FROM		(SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION
					FROM ALL_TAB_COLUMNS@DBLinksourceoracledb)									ATC
	INNER JOIN	ar_controltable															TL
	ON			ATC.TABLE_NAME											=			TL.sourceoracledbTableName
	WHERE		ATC.OWNER												=			TL.sourceoracledbSchema
)

SELECT		     controltable_sk
    ,           reconidentifier
    ,           projectidentifier
    ,           projectname
    ,           TableIdentifier
    
   ,         MIN(TableSource)														TableSource
,			MIN(OWNER)																OWNER

,			sourceoracledbTableName
,			targetoracledbTableName
,           sasDATASETNAME
,			COLUMN_NAME
,			DATA_TYPE
,			DATA_LENGTH
,			DATA_PRECISION
FROM		TableDetail
GROUP BY	controltable_sk ,reconidentifier,projectidentifier,projectname , TableIdentifier,sourceoracledbTableName, targetoracledbTableName,sasDATASETNAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION
HAVING		COUNT(*) = 1
--ORDER BY	controltable_sk ,reconidentifier,projectidentifier,projectname , TableIdentifier,sourceoracledbTableName, targetoracledbTableName, COLUMN_NAME
) newdata
 ON (olddata.controltable_sk=newdata.controltable_sk)
    when MATCHED then update set  olddata.reconidentifier=newdata.reconidentifier where 1=2
    when NOT MATCHED THEN
        INSERT 
        (
            CONTROLTABLE_SK ,
            RECONIDENTIFIER  ,
            PROJECTIDENTIFIER ,
            PROJECTNAME  ,
            TABLEIDENTIFIER  ,
            TABLESOURCE , 
            OWNER , 
            sourceoracledbTABLENAME , 
            targetoracledbTABLENAME , 
            sasDATASETNAME,
            COLUMN_NAME , 
            DATA_TYPE , 
            DATA_LENGTH , 
            DATA_PRECISION 
        )
        VALUES
        (
            newdata.CONTROLTABLE_SK ,
            newdata.RECONIDENTIFIER  ,
            newdata.PROJECTIDENTIFIER ,
            newdata.PROJECTNAME  ,
            newdata.TABLEIDENTIFIER  ,
            newdata.TABLESOURCE , 
            newdata.OWNER , 
            newdata.sourceoracledbTABLENAME , 
            newdata.targetoracledbTABLENAME , 
            newdata.sasDATASETNAME,
            newdata.COLUMN_NAME , 
            newdata.DATA_TYPE , 
            newdata.DATA_LENGTH , 
            newdata.DATA_PRECISION 
        )
;
COMMIT;
DBMS_OUTPUT.PUT_LINE('loaded data for sas structure comparsions');

END IF;

----------------------------------------------------sas Section------------------------------
IF  executesasrecon='Y' AND exeallcolumncrecon='Y' THEN
--Loads Mutual columns for sourceoracledb-sas
insert into ar_mutualcolumns
WITH 
 Transformation_sourceoracledb AS
(
				SELECT 1 TransformationID, 'REGEXP_REPLACE(SUBSTR(LTRIM(RTRIM([X])),1,[Y]),''[^0-9A-Za-zspace]'', '''') [Z]' sourceoracledbTransformationSQL FROM DUAL
    UNION ALL	SELECT 9, '[X] [Y]' FROM DUAL
)
, Transformation_sas AS
(
				SELECT 1 TransformationID, 'REGEXP_REPLACE(SUBSTR(LTRIM(RTRIM([X])),1,[Y]),''[^0-9A-Za-zspace]'', '''') [Z]' sasTransformationSQL FROM DUAL
  --  UNION ALL	SELECT 9, 'NVL([X],''NA'')' FROM DUAL
  UNION ALL	SELECT 9, '[X] [Y]'  FROM DUAL
),
sasColumns AS 
    (SELECT OWNER, TABLE_NAME, OWNER||'.'||TABLE_NAME QULAIFIED_TABLE_NAME ,COLUMN_NAME, OWNER||'.'||TABLE_NAME||'.'||COLUMN_NAME QUALIFIED_COLUMN_NAME,DATA_TYPE,COLUMN_ID ,TABLE_NAME||'.'||COLUMN_NAME TABLE_COLUMN_NAME ,CHAR_LENGTH FROM SYS.ALL_TAB_COLUMNS )
,sourceoracledbColumns AS     
    (SELECT OWNER, TABLE_NAME, OWNER||'.'||TABLE_NAME QULAIFIED_TABLE_NAME ,COLUMN_NAME, OWNER||'.'||TABLE_NAME||'.'||COLUMN_NAME QUALIFIED_COLUMN_NAME,DATA_TYPE,COLUMN_ID,TABLE_NAME||'.'||COLUMN_NAME TABLE_COLUMN_NAME ,CHAR_LENGTH   FROM SYS.ALL_TAB_COLUMNS@DBLinksourceoracledb)
, MutualColumns AS 
  (
	SELECT		/*+ PARALLEL(t,4) */
                TL.controltable_sk
    ,           TL.reconidentifier
    ,           TL.projectidentifier
    ,           TL.projectname
	,			TL.TableIdentifier
    ,			sas.COLUMN_NAME 
    ,           sas.DATA_TYPE targetoracledb_DATA_TYPE
    ,           '' TRANSFORMED_CLOUMN
    ,           ' ' targetoracledb_SCHEMA  
    ,           ' ' targetoracledb_COLUMN_ALIAS
	,			' '  targetoracledb_COLUMN
    ,           sourceoracledb.OWNER sourceoracledb_SCHEMA
    ,	        sourceoracledb.COLUMN_NAME sourceoracledb_COLUMN_ALIAS
    ,           COALESCE(REPLACE(REPLACE(REPLACE(T1.sourceoracledbTransformationSQL, '[X]',sourceoracledb.QUALIFIED_COLUMN_NAME),'[Y]',250),'[Z]',sourceoracledb.COLUMN_NAME), sourceoracledb.QUALIFIED_COLUMN_NAME) sourceoracledb_COLUMN --comparing char columns with max length of 255 only
    ,           sourceoracledb.DATA_TYPE sourceoracledb_DATA_TYPE
	,			sas.OWNER sas_schema  
	,           COALESCE(REPLACE(REPLACE(REPLACE(T2.sasTransformationSQL, '[X]',sas.QUALIFIED_COLUMN_NAME),'[Y]',250),'[Z]',sas.COLUMN_NAME), sas.QUALIFIED_COLUMN_NAME) sas_column 
	,			sas.data_type sas_data_type 
  	,			ROW_NUMBER() OVER(PARTITION BY TL.controltable_sk,TL.TableIdentifier ORDER BY sourceoracledb.COLUMN_ID, sas.COLUMN_ID)	ColumnOrder
	FROM		sasColumns												sas
	INNER JOIN	sourceoracledbColumns                                      		sourceoracledb
	ON		    sas.COLUMN_NAME											=			sourceoracledb.COLUMN_NAME
    AND     sas.OWNER =     targetoracledbusername
	INNER JOIN	ar_ControlTable															TL
	ON			sourceoracledb.TABLE_NAME											=			TL.sourceoracledbTableName
	AND			sourceoracledb.OWNER												=			TL.sourceoracledbSchema
	AND			sas.TABLE_NAME											=			TL.sasDATASETNAME
--	AND			sourceoracledb.OWNER												=			'FINAPP'
    AND         TL.sasRECON_IND='Y'--try removing this deep dependency later on and keep at high level for ease of matintinance 
    AND          sas.DATA_TYPE <>'CLOB'
   
LEFT JOIN	Transformation_sourceoracledb														T1
	ON			COALESCE( CASE
					WHEN	sourceoracledb.DATA_TYPE IN ( 'VARCHAR', 'VARCHAR2') THEN 1
                    WHEN    sourceoracledb.DATA_TYPE = 'CLOB' THEN 9 ELSE 5 END,999)	=			T1.TransformationID
LEFT JOIN	Transformation_sas														T2
	ON			COALESCE( CASE
					WHEN	sourceoracledb.DATA_TYPE IN ( 'VARCHAR', 'VARCHAR2') THEN 1
                    ELSE 5 END,999)	=			T2.TransformationID
)

, EXCLUDE_COLUMLIST AS (SELECT 
       t.controltable_sk,
        t.TABLEIDENTIFIER,
       v.column_value AS EXCLUDECOLUMN
FROM   ar_controltable t,
       TABLE( split_String( t.EXCLUDCOLUMNLIST ) ) v)
SELECT * FROM mutualcolumns T1 WHERE NOT EXISTS (SELECT 1 FROM EXCLUDE_COLUMLIST T2 WHERE T1.controltable_sk=t2.controltable_sk AND T1.column_name=T2.EXCLUDECOLUMN);
COMMIT;

DBMS_OUTPUT.PUT_LINE('loaded data for sas all column comparioson');

END IF; --Ending condition  executesasrecon='Y' AND exeallcolumncrecon='Y'


IF exeallcolumncrecon='Y' AND (executetargetoracledbrecon='Y' OR executesasrecon='Y' ) THEN--generates list for targetoracledb and sas columns 
---prepare comma seperated column list so it could it be used in dynamic sql statements.
INSERT INTO ar_MutualColumnList
WITH  MutualColumnListPrep AS
(
	SELECT		
                controltable_sk
    ,           reconidentifier
    ,           projectidentifier
    ,           projectname
    ,           TableIdentifier
	,			COLUMN_NAME
    ,           TRANSFORMED_CLOUMN
    ,           targetoracledb_SCHEMA
    ,           sourceoracledb_SCHEMA
    ,           targetoracledb_COLUMN
    ,           targetoracledb_COLUMN_ALIAS
	,			sourceoracledb_COLUMN
    ,           sourceoracledb_COLUMN_ALIAS
	,			CASE WHEN ColumnOrder <> 1
				THEN ', ' ELSE '' END												COMMA
   	,			ColumnOrder
    ,           targetoracledb_DATA_TYPE
    ,           sourceoracledb_DATA_TYPE
    ,           sas_schema
    ,           sas_column
    ,           sas_data_type
	FROM		ar_MutualColumns
)
SELECT		
              controltable_sk
    ,           reconidentifier
    ,           projectidentifier
    ,           projectname
,           TableIdentifier
,			dbms_xmlgen.convert(xmlagg(XMLELEMENT(E, COMMA||COLUMN_NAME) ORDER BY targetoracledb_SCHEMA,ColumnOrder).extract('//text()').getclobval() ,1)	MutualColumnList
,           dbms_xmlgen.convert(xmlagg(XMLELEMENT(E, COMMA||TRANSFORMED_CLOUMN) ORDER BY targetoracledb_SCHEMA,ColumnOrder).extract('//text()').getclobval() ,1) TransformedColumnList
,			dbms_xmlgen.convert(xmlagg(XMLELEMENT(E, COMMA||sourceoracledb_COLUMN) ORDER BY targetoracledb_SCHEMA,ColumnOrder).extract('//text()').getclobval() ,1)	sourceoracledbColumnList
,			dbms_xmlgen.convert(xmlagg(XMLELEMENT(E, COMMA||targetoracledb_COLUMN) ORDER BY targetoracledb_SCHEMA,ColumnOrder).extract('//text()').getclobval() ,1)	targetoracledbColumnList
,			dbms_xmlgen.convert(xmlagg(XMLELEMENT(E, COMMA||sourceoracledb_COLUMN_ALIAS) ORDER BY targetoracledb_SCHEMA,ColumnOrder).extract('//text()').getclobval() ,1)	sourceoracledbColumAliasList
,			dbms_xmlgen.convert(xmlagg(XMLELEMENT(E, COMMA||targetoracledb_COLUMN_ALIAS) ORDER BY targetoracledb_SCHEMA,ColumnOrder).extract('//text()').getclobval() ,1)	targetoracledbColumnAliasList
,			dbms_xmlgen.convert(xmlagg(XMLELEMENT(E, COMMA||sas_column) ORDER BY sas_schema,ColumnOrder).extract('//text()').getclobval() ,1)	sasColumnList
FROM		MutualColumnListPrep
GROUP BY	controltable_sk,reconidentifier,projectidentifier,projectname,  TableIdentifier
;
COMMIT;
DBMS_OUTPUT.PUT_LINE('loaded transformed comma seperated mutual column list');
END IF; --Ending condtion exeallcolumncrecon='Y' AND (executesasrecon='Y' OR exestructurecrecon='Y' )

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


