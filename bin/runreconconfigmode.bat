@echo off
set scripthome=C:\automation\sourceoracledbtargetoracledbrecon
call %scripthome%\systemconfig\configglobal.bat
call %userconfigpath%\configreconoptions.bat


set changeconnectionparameters=N
set changereconoptions=N

call %binpath%\runreconinteractivemode.bat %targetoracledbreconenv% %changeconnectionparameters% %changereconoptions% >%scripthome%\runreconinteractivemode.log
