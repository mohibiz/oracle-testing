 @echo off

	
		
		rem setting default inputs
	set targetoracledbnetservicename=
	set cdbnetservicename=
	set sourceoracledbnetservicename=
	
	set targetoracledbusername=
    set targetoracledbpassword=
	set cdbusername=
	set cdbpassword=
	set sourceoracledbusername=
	set sourceoracledbpassword=
	
	
			rem targetoracledbconfig parameters
		set targetoracledbhost=
		set targetoracledbport=1521
		set targetoracledbjdbcurl=%targetoracledbhost%:%targetoracledbport%/%targetoracledbnetservicename%
		set targetoracledbconnectionstring=%targetoracledbusername%/%targetoracledbpassword%@%targetoracledbjdbcurl% 
		
		rem cdb config parameters
		set cdbhost=
		set cdbport=1521
		set cdbjdbcurl=%cdbhost%:%cdbport%/%cdbnetservicename%
		set cdbconnectionstring=%cdbusername%/%cdbpassword%@%cdbjdbcurl% 
	
		rem sourceoracledb config parameters
		set sourceoracledbhost
		set sourceoracledbport=1521
		set sourceoracledbjdbcurl=%sourceoracledbhost%:%sourceoracledbport%/%sourceoracledbnetservicename%
		set sourceoracledbconnectionstring=%sourceoracledbusername%/%sourceoracledbpassword%@%sourceoracledbjdbcurl% 

		rem sas config parameters
		set sashost=
		set sasuser=
		set saspass=''

		
		