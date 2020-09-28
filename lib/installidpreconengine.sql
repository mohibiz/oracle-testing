SET SERVEROUTPUT ON;
PROMPT 'Installing reconciliation engine'

DEFINE libpath='&&1'; 
DEFINE logpath=&&2;
DEFINE logfilename=&&3;

SPOOL &&logpath/&&logfilename

SET echo on;
SET SERVEROUTPUT ON ;
SET timing on;

@&&libpath\proc_renamesastempdataset.sql
@&&libpath\proc_loadcommonmetadata.sql
@&&libpath\proc_generatemetdaforrecon.sql
@&&libpath\proc_recordcountcomparison.sql
@&&libpath\proc_allcolumncomparison.sql
@&&libpath\proc_structurerecomparioson.sql
@&&libpath\proc_arcleanup.sql


EXIT