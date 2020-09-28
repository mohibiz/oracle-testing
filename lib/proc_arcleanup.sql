/*To Do's */
--Write nested procedure to remove repeative code so the one procedure could be called to generated recon views for sourceoracledb and cdb.

CREATE OR REPLACE PROCEDURE proc_arcleanup(PARAM_LOG_SK NUMBER)
AS 
 CURSOR temptables IS
	SELECT	table_name 
	FROM		user_tables WHERE table_name like 'ART_%'	 											
	;
    
    CURSOR resulttables IS
	SELECT	table_name 
	FROM		user_tables WHERE table_name like 'AR_%'	 											
	;
  temptables_drop_stmt VARCHAR2(4000)  ;
  resulttables_trun_stmt VARCHAR2(4000)  ;
  
    BEGIN
  FOR c IN temptables
	LOOP
		BEGIN
        --drtoping ART_ tables
            temptables_drop_stmt:='DROP TABLE '||c.table_name;
            EXECUTE IMMEDIATE temptables_drop_stmt;
      
		EXCEPTION
		WHEN others THEN -- null;
        INSERT INTO CLEANUPERROS--(TableName,action,Message,LOG_SK)
     VALUES(c.table_name,temptables_drop_stmt ,SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000),PARAM_LOG_SK);
         
   END;
          
	END LOOP;
    
    FOR c IN resulttables
	LOOP
		BEGIN
         --Truncating AR_ tables
            resulttables_trun_stmt:='TRUNCATE TABLE '||c.table_name;
            EXECUTE IMMEDIATE resulttables_trun_stmt;
               
		EXCEPTION
		WHEN others THEN   -- null;
        INSERT INTO CLEANUPERROS--(TableName,action,Message,LOG_SK)
     VALUES(c.table_name,resulttables_trun_stmt,SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000),PARAM_LOG_SK);
         
   END;
          
	END LOOP;
    
    
    END;
 
/
EXIT