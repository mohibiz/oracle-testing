#Possible issue
#File does not exists 
#authentication issue
#connection parameters issue

$sasfile=$args[0]
$sasuser=$args[1]
$saspass=$args[2]
$targetoracledbnetservicename=$args[3]
$targetoracledbusername=$args[4]
$targetoracledbpassword=$args[5]

$objFactory = New-Object -ComObject SASObjectManager.ObjectFactoryMulti2
$objServerDef = New-Object -ComObject SASObjectManager.ServerDef
$objServerDef.MachineDNSName = "" # SAS Workspace node

$objServerDef.Port = 8791  # workspace server port

$objServerDef.Protocol = 2     # 2 = IOM protocol

# Class Identifier for SAS Workspace

$objServerDef.ClassIdentifier = "440196d4-90f0-11d0-9f41-00a024bb830c"


# create and connect to the SAS session 

$objSAS = $objFactory.CreateObjectByServer(
    "SASApp", # server name
    $true, 
    $objServerDef, # built server definition
    $sasuser, # user ID
    $saspass    # password
)

# program to run
# could be read from external file
#$program = "options formchar='|----|+|---+=|-/\<>*';"  
#$program += "ods listing; proc means data=sashelp.cars mean mode min max; run;"
$program ="%LET conn=PATH=$targetoracledbnetservicename USERNAME= $targetoracledbusername password= $targetoracledbpassword ;"
$program +="LIBNAME outsch targetoracledb &conn;  "
$program +="%LET schema=$targetoracledbusername;  "
$program +=Get-Content $sasfile;

# run the program
$objSAS.LanguageService.Submit($program);

# flush the output - could redirect to external file
Write-Output "Output:"
$list = ""
do {

    $list = $objSAS.LanguageService.FlushList(1000)

    Write-Output $list

} while ($list.Length -gt 0)

# flush the log - could redirect to external file

Write-Output "LOG:"

$log = ""

do {

    $log = $objSAS.LanguageService.FlushLog(1000)

    Write-Output $log

} while ($log.Length -gt 0)



# end the SAS session

$objSAS.Close()