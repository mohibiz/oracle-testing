# Automation Atomic Reconciliation

**Preinstallation:**

For the conformity of functionality of the bundle, it should be copied in a specific folder. Following are the steps for the same:

- Create a folder named &quot; **automation**&quot; in your C: drive

(If you are working in Citrix, go to run bar, type C:\ and enter, it will open the C:\ drive)

- Copy the &quot; **sourceoracledbtargetoracledbrecon**&quot; from Github repository &quot; **dart-phoenix-remediation**&quot; under oracle_targetoracledb/atomic to the newly formed &quot;automation&quot; folder in C: drive. 

**Installation:**

Change the system config for the first time only:

1. Open sourceoracledbtargetoracledbrecon  systemconfig
2. Right click configglobal.bat file and open with editor
3. Set sqldevhome to the SQL developer bin folder which has sql.exe file. The SQL developer folder is generally present in the C: drive of your local machine.
4. Set the scripthome to the location of sourceoracledbtargetoracledbrecon folder. By default if you follow the preinstallation the scripthome will be similar &quot;C:\automation\sourceoracledbtargetoracledbrecon&quot;.
5. Save the file and close it.
6. Next, change the configtargetoracledbe2e.bat file. For this, right click the configtargetoracledbe2e.bat and open with editor.
7. Set the targetoracledbusername to your targetoracledb E2E username and set the targetoracledbpassword to your targetoracledb E2E password.
8. Save the file and close it.
9. Finally, change the configtargetoracledbt01.bat file. For this, right click the file and open with editor.
10. Set the targetoracledbusername to your targetoracledb T01 username and targetoracledbpassword to your targetoracledb T01 password. Save the changes and close the file.

Note: Make sure that the loadcontrolfile.sql has the path of the Reconcontrol table CSV file. By default, it will be set to &quot;C:\automation\sourceoracledbtargetoracledbrecon\userconfig\RECONCONTROLTABLE.csv&quot;. If there are changes in the script home in step 4 of this section then the path for CSV file has to be changed.

Next, change the userconfig folder:

1. Open sourceoracledbtargetoracledbrecon  userconfig
2. Right click the configreconoptions.bat file and open with editor.
3. Set the targetoracledbreconenv either to &quot; **targetoracledbT01**&quot; or &quot; **targetoracledbE2E** or &quot; **targetoracledbN02**&quot;
4. For the first time, set the installreconobjects to &quot;Y&quot;. Set the other options according to the following requirements:

| **Option** | **Requirement** |
| --- | --- |
| Exestructurerecon | To execute the tables&#39; structure comparison in DB |
| Exerowcountrecon | To execute the tables&#39; record count comparison in DB |
| Exeallcolumncrecon | To execute columns comparison in DB |
| Executecleanup | To drop temporary tables and truncating the permanent tables. (Preferably &quot;N&quot; for first time.) |

1. Save the changes and close the file.
2. Next, change the &quot; **RECONCONTROLTABLE.csv**&quot; according to the requirements of the reconciliation process and save it.

**Run in Configured Mode (First time):**

The file will run in the pre-configured mode. For this set the test environment to be used.

- Open sourceoracledbtargetoracledbrecon  bin
- Right click the runreconconfigmode.bat file and open with editor.
- Keep the other options, changeconnectionparameters and changereconoptions to &quot;N&quot;.
- Save the changes and close the file.

Run the runsourceoracledbtargetoracledbreconconfigmode.bat file by double clicking the file.

**Run in Configured Mode (Following first time):**

Following the first time, the user config files should be changed. Following are the changes:

- In the userconfig folder (sourceoracledbtargetoracledbrecon  userconfig), right click the configreconoptions.bat file and open with editor.
- Set the installreconobjects to &quot; **Y**&quot;. Change the other options according to your requirements. Save the file.
- Change the &quot; **RECONCONTROLTABLE.csv**&quot; according to the new requirements and save the changes.

Run the runsourceoracledbtargetoracledbreconconfigmode.bat file (sourceoracledbtargetoracledbrecon  bin) by double clicking the file.

Note: if you want to change the test environment, right click the runsourceoracledbtargetoracledbreconconfigmode.bat file, change the targetoracledbreconenv and save the file.

**Recon control table:**

Following are the columns of the &quot; **RECONCONTROLTABLE.csv**&quot; and the description:

| **Column Name** | **Description** |
| --- | --- |
| RECONIDENTIFIER\* | The column identifies the number of times a table object is reconciled in the given execution. For example, if a table &quot;Tab1&quot; has more than one instances with different conditions. The first instance will be valued &quot;1&quot;, second will be &quot;2&quot; and so on. |
| PROJECTIDENTIFIER\* | The project code for reconciliation. |
| PROJECTNAME\* | Name of the project. |
| TABLEIDENTIFIER\* | Unique number for each instance of the execution per table. |
| sourceoracledbCHEMA\* | sourceoracledb schema of the table. |
| sourceoracledbTABLENAME\* | sourceoracledb table name. |
| CDBSCHEMA\* | CDB schema containing table. |
| CDBTABLENAME\* | CDB table name. |
| targetoracledbTABLENAME\_sourceoracledb\_TA1\* | targetoracledb table name. It can be a TA1 table or TL1 table. |
| targetoracledbTABLENAME\_NGCM\_SA2 | The schema is not used for this reconciliation. ( **Do not include any value** ) |
| TABLEKEYS\* | The comma separated primary/composite keys to identify each record. |
| DATAVAR | Condition to filter the records for reconciliation. Default is TRUNC(LastUpdateDate). The value can be NULL and will not have any filtering. |
| DATEGREATERTHAN | Column specifies condition greater than equal to column value DATAVAR. |
| DATELESSTHAN | Column specifies condition lesser than equal to column value DATAVAR. |
| DATENOTIN | Column specifies list of values excluded while filtering using DATAVAR. |
| EXTRACONDITION | Includes an extra condition to filter the records.The value should be given in SQL syntax starting from &quot;AND&quot;. For example, to filter records of one value of I, the value for the column will be &quot;AND I = 123456&quot;. |
| EXCLUDECOLUMNLIST | Comma separated values of table columns to be excluded from the reconciliation. |
| RECON\_IND\* | Binary column with &quot;Y&quot; or &quot;N&quot; value for either including the instance in reconciliation or not. |
| COMMENTS | Include comments in the control table for reference. |

The columns marked (\*) are mandatory fields for execution.

**Result Tables:**

Permanent tables for Results:

Following are the permanent tables and their descriptions

| Table name | Columns | Description |
| --- | --- | --- |
| AR\_ALLCOLUMNERRORS | CONTROLTABLE\_SK, LOG\_SK, MESSAGE, PROJECTIDENTIFIER, PROJECTNAME, RECONIDENTIFIER, RECONNAME, TABLEIDENTIFIER, TABLENAME | Column comparison errors during execution. |
| AR\_ALLCOLUMNRESULTS | CDBTABLENAME, CDB\_targetoracledb\_RECONCOUNT, COMPLETEDDATETIME, COMPLETEDDURATION, CONTROLTABLE\_SK, sourceoracledbTABLENAME, sourceoracledb\_targetoracledb\_DUPLICATES, sourceoracledb\_targetoracledb\_RECONCOUNT, ERRORDATETIME, targetoracledbTABLENAME\_sourceoracledb\_SA1, targetoracledbTABLENAME\_NGCM\_SA2, INITIATEDDATETIME, LOG\_SK, PROJECTIDENTIFIER, PROJECTNAME, RECONIDENTIFIER, SELECTSQL, TABLEIDENTIFIER | Column comparison results. |
| AR\_CONTROLTABLE | CDBSCHEMA, CDBTABLENAME, COMMENTS, CONTROLTABLE\_SK, DATAVAR, DATEGREATERTHAN, DATELESSTHAN, DATENOTIN, sourceoracledbSCHEMA, sourceoracledbTABLENAME, EXCLUDCOLUMNLIST, EXTRACONDITION, targetoracledbTABLENAME\_sourceoracledb\_SA1, targetoracledbTABLENAME\_NGCM\_SA2, PROJECTIDENTIFIER, PROJECTNAME, RECONIDENTIFIER, RECON\_IND, TABLEIDENTIFIER, TABLEKEYS | Selected tables on which the reconciliation was done as specified by RECON\_IND. |
| AR\_EXECUTION\_CONTROLER | EXECUTION\_ID, EXECUTION\_TIMESTAMP | The execution of current process. |
| AR\_EXECUTION\_LOG | COMMENTS, EXECUTION\_BK, EXECUTION\_ERROR, EXECUTION\_STATUS, LOG\_SK, PROCEDURE\_NME, RECON\_END\_DTE, RECON\_START\_DTE | Provides LOG\_SK and EXECUTION\_ID for present and previous executions. |
| AR\_EXECUTION\_REQUEST | CDBNETSERVICENAME, COMMENTS, CREATED\_BY, CREATED\_DTE, sourceoracledbNETSERVICENAME, EXEALLCOLUMNCRECON, EXECUTION\_SK, EXEROWCOUNTCRECON, EXESTRUCTURECRECON, targetoracledbNETSERVICENAME, REQUEST\_END\_DTE, REQUEST\_STATUS, REQUEST\_SUBMIT\_DTE | Describe the present and previous executions. |
| AR\_MUTUALCOLUMNLIST | CONTROLTABLE\_SK, sourceoracledbCOLUMALIASLIST, sourceoracledbCOLUMNLIST, targetoracledbCOLUMNALIASLIST, targetoracledbCOLUMNLIST, MUTUALCOLUMNLIST, PROJECTIDENTIFIER, PROJECTNAME, RECONIDENTIFIER, TABLEIDENTIFIER, TRANSFORMEDCOLUMNLIST | Provides comma separated mutual column list for column comparison. |
| AR\_MUTUALCOLUMNLOBLIST | COMPARELOBCOLUMNLIST, CONTROLTABLE\_SK, sourceoracledbCOLUMNALIASLIST, sourceoracledbCOLUMNLIST, targetoracledbCOLUMNALIASLIST, targetoracledbCOLUMNLIST, LOBRECONCONDITION, MUTUALCOLUMNLIST, PROJECTIDENTIFIER, PROJECTNAME, RECONIDENTIFIER, TABLEIDENTIFIER, TRANSFORMEDCOLUMNLIST | Provides comma separated mutual column list for column comparison. |
| AR\_MUTUALCOLUMNLOBS | COLUMNORDER, COLUMN\_NAME, COMMA, CONTROLTABLE\_SK, sourceoracledb\_COLUMN, sourceoracledb\_COLUMN\_ALIAS, sourceoracledb\_DATA\_TYPE, sourceoracledb\_SCHEMA, targetoracledb\_COLUMN, targetoracledb\_COLUMN\_ALIAS, targetoracledb\_DATA\_TYPE, targetoracledb\_SCHEMA, PROJECTIDENTIFIER, PROJECTNAME, RECONIDENTIFIER, TABLEIDENTIFIER, TRANSFORMED\_CLOUMN, TRANSFORMED\_LOB\_CLOUMN, TRANSFORMED\_LOB\_CLOUMN\_ALIAS | Provides all the CLOB columns in the tables. |
| AR\_MUTUALCOLUMNS | COLUMNORDER, COLUMN\_NAME, CONTROLTABLE\_SK, sourceoracledb\_COLUMN, sourceoracledb\_COLUMN\_ALIAS, sourceoracledb\_DATA\_TYPE, sourceoracledb\_SCHEMA, targetoracledb\_COLUMN, targetoracledb\_COLUMN\_ALIAS, targetoracledb\_DATA\_TYPE, targetoracledb\_SCHEMA, PROJECTIDENTIFIER, PROJECTNAME, RECONIDENTIFIER, TABLEIDENTIFIER, TRANSFORMED\_CLOUMN | Provides all columns in the tables. |
| AR\_RECONCONTROLTABLECSV | CDBSCHEMA, CDBTABLENAME, COMMENTS, DATAVAR, DATEGREATERTHAN, DATELESSTHAN, DATENOTIN, sourceoracledbSCHEMA, sourceoracledbTABLENAME, EXCLUDCOLUMNLIST, EXTRACONDITION, targetoracledbTABLENAME\_sourceoracledb\_SA1, targetoracledbTABLENAME\_NGCM\_SA2, PROJECTIDENTIFIER, PROJECTNAME, RECONIDENTIFIER, RECON\_IND, TABLEIDENTIFIER, TABLEKEYS | RECONCONTROLTABLE.CSV file created by user. |
| AR\_RECORDCOUNTERRORS | CONTROLTABLE\_SK, LOG\_SK, MESSAGE, PROJECTIDENTIFIER, PROJECTNAME, RECONIDENTIFIER, TABLEIDENTIFIER, TABLENAME, TABLESOURCE | Errors encountered during record count comparisons. Shows the history also. |
| AR\_RECORDCOUNTRESULTS | CDBRECORDCOUNT, CDBTABLENAME, COMPLETEDDATETIME, COMPLETEDDURATION, CONTROLTABLE\_SK, sourceoracledbRECORDCOUNT, sourceoracledbTABLENAME, ERRORDATETIME, targetoracledbTABLENAME, targetoracledb\_sourceoracledb\_SA1\_RECORDCOUNT, targetoracledb\_NGCM\_SA2\_RECORDCOUNT, INITIATEDDATETIME, LOG\_SK, PROJECTIDENTIFIER, PROJECTNAME, RECONIDENTIFIER, RECORDCOUNTDIFFERENCE, TABLEIDENTIFIER | Results of record count comparisons. Shows the results of previous executions also. |
| AR\_STRUCTUREERRORS | CONTROLTABLE\_SK, LOG\_SK, MESSAGE, PROJECTIDENTIFIER, PROJECTNAME, RECONIDENTIFIER, TABLEIDENTIFIER, TABLENAME, TABLESOURCE | Errors encountered during structure comparison. |
| AR\_STRUCTURERESULTS | CDBTABLENAME, CDB\_targetoracledb\_COLUMN\_MISMATCH, CDB\_targetoracledb\_DATALENGTH\_MISMATCH, CDB\_targetoracledb\_DATA\_PREC\_MISMATCH, CDB\_targetoracledb\_DATETYPE\_MISMATCH, COMPLETEDDATETIME, COMPLETEDDURATION, CONTROLTABLE\_SK, sourceoracledbTABLENAME, sourceoracledb\_targetoracledb\_COLUMN\_MISMATCH, sourceoracledb\_targetoracledb\_DATALENGTH\_MISMATCH, sourceoracledb\_targetoracledb\_DATA\_PREC\_MISMATCH, sourceoracledb\_targetoracledb\_DATETYPE\_MISMATCH, ERRORDATETIME, targetoracledbTABLENAME\_sourceoracledb\_SA1, INITIATEDDATETIME, LOG\_SK, PROJECTIDENTIFIER, PROJECTNAME, RECONIDENTIFIER, SELECTSQL, TABLEIDENTIFIER | Results of structure comparisons. Shows the history of the previous executions also. |
| AR\_STRUCVAL\_CDB\_TO\_targetoracledb\_sourceoracledb\_SA1 | CDBTABLENAME, COLUMN\_NAME, CONTROLTABLE\_SK, DATA\_LENGTH, DATA\_PRECISION, DATA\_TYPE, targetoracledbTABLENAME, OWNER, PROJECTIDENTIFIER, PROJECTNAME, RECONIDENTIFIER, TABLEIDENTIFIER, TABLESOURCE |
 |
| AR\_STRUCVAL\_sourceoracledb\_TO\_targetoracledb\_sourceoracledb\_SA1 | COLUMN\_NAME, CONTROLTABLE\_SK, DATA\_LENGTH, DATA\_PRECISION, DATA\_TYPE, sourceoracledbTABLENAME, targetoracledbTABLENAME, OWNER, PROJECTIDENTIFIER, PROJECTNAME, RECONIDENTIFIER, TABLEIDENTIFIER, TABLESOURCE |
 |
| CLEANUPERROS | LOG\_SK, MESSAGE, SQL\_STMT, TABLENAME | Error logs during clean up. |
| LOBRECONRESULTS | FK\_ED\_I\_targetoracledb, FK\_ED\_C\_targetoracledb, ADVICE\_TEXT\_targetoracledb, FK\_ED\_I, FK\_ED\_C, ADVICE\_TEXT, COMPARE\_ADVICE\_TEXT | CLOB data comparison between sourceoracledb and targetoracledb tables. |
| CDBLOBRECONRESULTS | FK\_ED\_I\_targetoracledb, FK\_ED\_C\_targetoracledb, ADVICE\_TEXT\_targetoracledb, FK\_ED\_I, FK\_ED\_C, ADVICE\_TEXT, COMPARE\_ADVICE\_TEXT | CLOB data comparison between CDB and targetoracledb tables. |