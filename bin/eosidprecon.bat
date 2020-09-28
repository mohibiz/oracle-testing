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
	


	::=======================Execution Section
	::========================Insert the recon execution log
	rem add arguments for sas and targetoracledb recon 
	rem add two more variable here executesasrecon ,executesasrecon
		%executesqlscript% @%libpath%\insertprocesslog.sql %exestructurecrecon% %exerowcountcrecon% %exeallcolumncrecon% %sourceoracledbnetservicename% %targetoracledbnetservicename% %cdbnetservicename% %targetoracledbusername% %logpath% 'sourceoracledbtargetoracledbrecon' insertprocesslog.log
	
		rem populate metatada for reconciliation based on control file 
	echo "generating metadata for targetoracledb recon"  
	%executesqlscript% @%libpath%\executegeneratemetadataforrecon.sql %exestructurecrecon% %exerowcountcrecon% %exeallcolumncrecon% 'Y' 'N' %logpath% targetoracledb_executegeneratemetadataforrecon.log
	
		
	if %exerowcountcrecon%==Y (
		echo "Executing row count Reconciliation" 
		%executesqlscript% @%libpath%\executerowcountcomparison.sql  %logpath% targetoracledb_executerowcountcomparison.log 'Y' 'N'
		)
		
	  
	if %exeallcolumncrecon%==Y (
		echo "Executing all column Reconciliation" 
		%executesqlscript% @%libpath%\executeallcolumncomparison.sql %logpath% targetoracledb_executeallcolumncomparison.log 'Y' 'N'
		)
	
	if %exestructurecrecon%==Y (
		echo "Executing table structure Reconciliation" 
		%executesqlscript% @%libpath%\executestructurecomparison.sql %logpath% targetoracledb_executestructurecomparison.log 'Y' 'N'
		)	
	
	::========================Insert the recon execution log		
		%executesqlscript% @%libpath%\insertprocesslog.sql %exestructurecrecon% %exerowcountcrecon% %exeallcolumncrecon% %sourceoracledbnetservicename% %targetoracledbnetservicename% %cdbnetservicename% %targetoracledbusername% %logpath% 'sourceoracledbtargetoracledbrecon' insertprocesslog.log
	
	:end
	
    