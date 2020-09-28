DEFINE logpath=&&1;
DEFINE logfilename=&&2;

SPOOL &&logpath/&&logfilename

SET echo on;
SET SERVEROUTPUT ON ;
SET timing on;

PROMPT 'Truncating and loading recon control csv file'

--Truncate Control Tables
TRUNCATE TABLE AR_RECONCONTROLTABLECSV;

--Load Control File
LOAD AR_RECONCONTROLTABLECSV C:\automation\sourceoracledbtargetoracledbrecon\userconfig\RECONCONTROLTABLE.csv;
begin
dbms_output.put_line('recon control csv file loaded' );
end;

/
EXIT;











