 ::=============================Enviornement Setup Section
 
	set scripthome=C:\automation\sourceoracledbtargetoracledbrecon
	echo %scripthome%\systemconfig
	call %scripthome%\systemconfig\configglobal.bat
	call %userconfigpath%\configreconoptions.bat

::=============================Input and Config Section
	
	
	echo choose enviornment for reconciliation
	
	if [%1] == [] (
		set /p targetoracledbreconenv="targetoracledb enviornment  (targetoracledbdb): Default is targetoracledbDb :"%1
		)
		
	if %targetoracledbreconenv%==targetoracledbdb (call %systemconfigpath%\configtargetoracledbdb.bat)
	
	echo %targetoracledbreconenv%
	
	echo verify connection details...
		echo %targetoracledbconnectionstring%
		echo %cdbconnectionstring%
		echo %sourceoracledbconnectionstring%
	if [%2] == [] (
		set /p changeconnectionparameters="Would you like to change connection parameters (Y/N): "
		)
		
	if %changeconnectionparameters%==Y (
		set /p targetoracledbhost="enter targetoracledb host :"
		set /p targetoracledbport="enter targetoracledb port :"
		set /p targetoracledbnetservicename="enter targetoracledb service :"
		set /P targetoracledbusername="enter targetoracledb username : "
		set /P targetoracledbpassword="enter targetoracledb password : "
		set targetoracledbjdbcurl=%targetoracledbhost%:%targetoracledbport%/%targetoracledbnetservicename%
		set targetoracledbconnectionstring=%targetoracledbusername%/%targetoracledbpassword%@%targetoracledbjdbcurl% 
		
		rem cdb config parameters
		set /p cdbhost="enter cdb host :"
		set /p cdbport="enter cdb port :"
		set /p cdbnetservicename="enter cdb service :"
		set /P cdbusername="enter cdb username : "
		set /P cdbpassword="enter cdb password : "
		set cdbjdbcurl=%cdbhost%:%cdbport%/%cdbnetservicename%
		set cdbconnectionstring=%cdbusername%/%cdbpassword%@%cdbjdbcurl% 
		
		rem sourceoracledb config parameters
		set /p sourceoracledbhost="enter sourceoracledb host :"
		set /p sourceoracledbport="enter sourceoracledb port :"
		set /p sourceoracledbnetservicename="enter sourceoracledb service :"
		set /P sourceoracledbusername="enter sourceoracledb username : "
		set /P sourceoracledbpassword="enter sourceoracledb password : "
		set sourceoracledbjdbcurl=%sourceoracledbhost%:%sourceoracledbport%/%sourceoracledbnetservicename%
		set sourceoracledbconnectionstring=%sourceoracledbusername%/%sourceoracledbpassword%@%sourceoracledbjdbcurl% 
	
	rem ***add section for sas connections.report if not added***
		echo verify connection details...
		echo targetoracledb connection is : %targetoracledbconnectionstring%
		echo cdb connection is : %cdbconnectionstring%
		echo sourceoracledb connection is : %sourceoracledbconnectionstring%
		)
	

	
	if [%3] == [] (
		set /p changereconoptions="Would you like to change recon options(Y/N): "
		)
		
	if %changereconoptions%==Y (
		Set /p installreconobjects="Would you like to install recon objects (Y/N): Default is N :"
		set /p exestructurecrecon="Would you like to execute Structure recon(Y/N): Default is Y :"
		set /p exerowcountcrecon="Would you like to execute row count recon(Y/N): Default is Y :"
		set /p exeallcolumncrecon="Would you like to execute all column recon(Y/N): Default is Y :"
		)
		
		echo verify recon options...
	
		echo install recon objects    %installreconobjects%
		echo cleanup recon objects    %executearcleanup%
		echo execute Structure recon  %exestructurecrecon%
		echo execute row count recon %exerowcountcrecon%
		echo all column recon %exeallcolumncrecon%	
		
		
	
		::========================Setup section
		set executesqlscript=start /wait %sqldevhome%\sql.exe %targetoracledbconnectionstring% 
		
		::========================Cleanup section
		rem remove log files for installation folder in every run
		del /Q /S %logpath%\*
		
		rem cleaup the reconciliation - This needs to be checked . cleanup should be done only in the begining and has to be sepereated from main execution scripts otherwise they will cleanup each others results
		rem clean should be common for both recons as the results and temp tables are stored in same schema and tables 
		if %executearcleanup%==Y (
		echo "Executing cleanup of Reconciliation" 
		%executesqlscript% @%libpath%\executearcleanup.sql %logpath% executearcleanup.log
		)
		

		::========================Initialize section
		rem this will be common initializeprocess to facilitate ,this process creates execution id which control the whole exeuction process
		echo %logpath%
		
		%executesqlscript% @%libpath%\initializeprocess.sql %logpath% initializeprocess.log
	
		 
		
		::========================Installtion Section	
		rem common installatin section as targetoracledb and sas recon shares the common objects for execution
	if %installreconobjects%==Y  (
		%executesqlscript% @%libpath%\installtargetoracledbreconobjects.sql %cdbusername% %cdbpassword% %cdbjdbcurl% %sourceoracledbusername% %sourceoracledbpassword% %sourceoracledbjdbcurl% %logpath% installtargetoracledbreconobjects.log
		::there is dependencies between recon tables and recon engine ,make sure that recon tables gets created first.hence placing a delay to allow tables to be created
		::In ideal world there should be a check before execution of engine whether objects exists or not. if not then do not execute and exit
	
		%executesqlscript% @%libpath%\installtargetoracledbreconengine.sql %libpath% %logpath% installtargetoracledbreconengine.log
		)
		
				
	::========================Insert the recon execution log
	rem add arguments for sas and targetoracledb recon 
	rem	%executesqlscript% @%libpath%\insertprocesslog.sql %exestructurecrecon% %exerowcountcrecon% %exeallcolumncrecon% %sourceoracledbnetservicename% %targetoracledbnetservicename% %cdbnetservicename% %targetoracledbusername% %logpath% insertprocesslog.log
	
	rem check to see if common metadata needs to be loaded or not
	set runmetadatasection=N
	if %executetargetoracledbrecon%==Y set runmetadatasection=Y
	if %executesasrecon%==Y set runmetadatasection=Y
	
	if %runmetadatasection%==Y (
	::=======================Loading Metadata Section
	rem load config file in database , this requires error handling so if the control file is not loaded then it shouldnt execute recon
	echo "loading csv table for recon"  
	%executesqlscript% @%systemconfigpath%\loadcontrolfile.sql %logpath% loadcontrolfile.log
	
	rem load common metadata
	echo "loading loading common metdata to be able to generate metdata for recon"  
	%executesqlscript% @%libpath%\loadcommonmetadata.sql %exestructurecrecon% %exerowcountcrecon% %exeallcolumncrecon% %executetargetoracledbrecon% %executesasrecon% %logpath% loadcommonmetadata.log
	)
	
		
	::=======================Execution Section
	
	if "%executetargetoracledbrecon%"=="Y" (
		echo calling targetoracledbrecon
		call %binpath%\sourceoracledbtargetoracledbrecon.bat %targetoracledbreconenv% %changeconnectionparameters% %changereconoptions%
	) 
	
	if "%executesasrecon%"=="Y" (
		echo calling sasrecon
		 call %binpath%\sourceoracledbsasrecon.bat %targetoracledbreconenv% %changeconnectionparameters% %changereconoptions%
	) 
	
	::========================Insert the recon execution log		
	rem %executesqlscript% @%libpath%\insertprocesslog.sql %exestructurecrecon% %exerowcountcrecon% %exeallcolumncrecon% %sourceoracledbnetservicename% %sasnetservicename% %cdbnetservicename% %targetoracledbusername%
	
