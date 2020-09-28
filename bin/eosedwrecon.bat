::===============================================================
:: Script 			: 
:: Author 			: 
:: Date   			:
:: Purpose			:
:: Modifications 	: 
:: Inputs required 	: 
::===============================================================

::find out ,how to handle the dependencies here 
::===============================================================
:: Potential Errors
::connection to database ie.wrong service name , wrong credentails 
::records are not loaded from csv file correctly , unqiue constraint error , 
::===============================================================

 @echo off
 
 ::=============================Enviornement Setup Section
 
	set scripthome=C:\automation\sourceoracledbtargetoracledbrecon
	echo %scripthome%\systemconfig
	call %scripthome%\systemconfig\configglobal.bat
	call %userconfigpath%\configreconoptions.bat
	

 ::=============================Input and Config Section
		
	rem moved to main script runreconinteractivemode.bat
	
	::=======================Execution Section
	
	rem pull tables from sas which are required to be reconciled
	rem could possibly do a loop here based on on either csv file or controltable in targetoracledb if recon needs to be started as soon as table is created
	rem for now just pull all the tabels first and then start the recon
		
	rem populate metatada for reconciliation based on control file 
	::========================Insert the recon execution log
	rem add arguments for sas and targetoracledb recon 
	rem add two more variable here executesasrecon ,executesasrecon
		%executesqlscript% @%libpath%\insertprocesslog.sql %exestructurecrecon% %exerowcountcrecon% %exeallcolumncrecon% %sourceoracledbnetservicename% %targetoracledbnetservicename% %cdbnetservicename% %sasuser% %logpath% 'sourceoracledbsasrecon' insertprocesslog.log
	set runsasprograms=N
	if %exerowcountcrecon%==Y set runsasprograms=Y
	if %exeallcolumncrecon%==Y set runsasprograms=Y
	if %exestructurecrecon%==Y set runsasprograms=Y
	
	if %runsasprograms%==Y (
	::===================== Run sas Programs
	::get the sasdataset first before generating metadata for recon
	echo "upload control table to sas for processing"
	powershell -command %libpath%\runsasprogram.ps1 %libpath%\gettargetoracledbtable.sas %sasuser% %saspass% %targetoracledbnetservicename% %targetoracledbusername% %targetoracledbpassword% > %logpath%\gettargetoracledbtable.log
	echo "download sasdataset to targetoracledb local user"
	powershell -command %libpath%\runsasprogram.ps1 %libpath%\uploadsasdataset.sas	%sasuser% %saspass% %targetoracledbnetservicename% %targetoracledbusername% %targetoracledbpassword% > %logpath%\uploadsasdataset.log
	
	
	rem populate metatada for reconciliation based on control file 
	echo "generating metadata for targetoracledb recon"  
	%executesqlscript% @%libpath%\executegeneratemetadataforrecon.sql %exestructurecrecon% %exerowcountcrecon% %exeallcolumncrecon% 'N' 'Y' %logpath% sas_executegeneratemetadataforrecon.log
	)
	if %exerowcountcrecon%==Y (
		echo "Executing row count Reconciliation" 
		 %executesqlscript% @%libpath%\executerowcountcomparison.sql  %logpath% sas_executerowcountcomparison.log 'N' %executesasrecon%
		)
		
	  
	if %exeallcolumncrecon%==Y (
		echo "Executing all column Reconciliation" 
		%executesqlscript% @%libpath%\executeallcolumncomparison.sql %logpath% sas_executeallcolumncomparison.log 'N' %executesasrecon%
		)
	
	if %exestructurecrecon%==Y (
		echo "Executing table structure Reconciliation" 
		 %executesqlscript% @%libpath%\executestructurecomparison.sql %logpath% sas_executestructurecomparison.log 'N' %executesasrecon%
		)	
		
		::========================Insert the recon execution log		
		%executesqlscript% @%libpath%\insertprocesslog.sql %exestructurecrecon% %exerowcountcrecon% %exeallcolumncrecon% %sourceoracledbnetservicename% %targetoracledbnetservicename% %cdbnetservicename% %targetoracledbusername% %logpath% 'sourceoracledbsasrecon' insertprocesslog.log
	
	rem rename temp tables created for sasdatasets
	%executesqlscript% @%libpath%\executerenamesastempdataset.sql  %logpath% sas_executerenamesastempdataset.log 'N' %executesasrecon%
	
	
	
	:end
	
    