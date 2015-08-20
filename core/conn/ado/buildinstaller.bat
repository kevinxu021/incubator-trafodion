@echo on
@REM setup system environment

@echo preparing...
if not exist "%VS100COMNTOOLS%" @(
	@echo *** Error: VsualStudio 2010 is required
	goto failedend
)

@call buildinfo.bat
@set "PATH=%InstallShield_Home%\System;%PATH%"

:building
@echo building...
@Call "%VS100COMNTOOLS%vsvars32.bat"
@RD /S /Q HP.Data.Neoview\bin
@DEL /S /F /Q %build_dir%\installer
@mkdir "%build_dir%\installer"
@if "%REVISION%"=="" @(
	@for /f "tokens=2" %%a in ('svn info ^| findstr /i revision') DO @set REVISION=%%a
)
@echo REVISION is %REVISION% 

@perl update_version.pl %Major% %Minor% %SPVersion% %REVISION% "%SeaQuest_Release%" "./HP.Data.Neoview/Public/ProductVersion.cs"
@perl update_version.pl %Major% %Minor% %SPVersion% %REVISION% "%SeaQuest_Release%" "./HP.Data.Neoview/Public/DisplayVersion.cs"

@DEL /F /Q %build_dir%\HP.Data.dll
@DEL /F /Q %build_dir%\HP.Data.ETL.dll
@DEL /F /Q %build_dir%\HP.Data.VisualStudio.dll
@DEL /F /Q %build_dir%\HP.Data.Installation.dll

@DEL /F /Q Release\HP.Data.dll
@DEL /F /Q Release\HP.Data.ETL.dll
@DEL /F /Q Release\HP.Data.VisualStudio.dll
@DEL /F /Q Release\HP.Data.Installation.dll
@devenv NeoviewProvider.sln /Build "release" /Project HP.Data
@copy Release\HP.Data.dll "%build_dir%"
if not exist "%build_dir%\HP.Data.dll" goto failedend

@devenv NeoviewProvider.sln /Build "release" /Project HP.Data.ETL
@copy Release\HP.Data.ETL.dll "%build_dir%"
if not exist "%build_dir%\HP.Data.ETL.dll" goto failedend

@devenv NeoviewProvider.sln /Build "release" /Project HP.Data.VisualStudio
@copy Release\HP.Data.VisualStudio.dll "%build_dir%"
if not exist "%build_dir%\HP.Data.VisualStudio.dll" goto failedend

@devenv NeoviewProvider.sln /Build "release" /Project HP.Data.Installation
@copy Release\HP.Data.Installation.dll "%build_dir%"
if not exist "%build_dir%\HP.Data.Installation.dll" goto failedend

@set ERRORLEVEL=0
@IsCmdBld -p installer\ADO.NET.ism -l PATH_TO_HPADO_FILES="%build_dir%" -j 2.0.50727 -c COMP -b "%build_dir%\installer" -y %ADO_NET_Version%.%REVISION% -r ADO.NET -f ADO.NET -z "ProductName=HP ADO.NET %ADO_NET_Version% PACKAGE" -z "ADO_NET_VERSION=%ADO_NET_Version%" -z "RELEASE_VERSION=%SeaQuest_Release%" -z "SVN_REVISION=%REVISION%"
@if exist "%build_dir%\installer\PROJECT_ASSISTANT\ADO.NET\DiskImages\DISK1\setup.exe" @(
	@copy "%build_dir%\installer\PROJECT_ASSISTANT\ADO.NET\DiskImages\DISK1\setup.exe" "%build_dir%\installer\HP_ADO.NET_%ADO_NET_Version%.exe"
)
@if exist "%build_dir%\installer\PROJECT_ASSISTANT\ADO.NET\DiskImages\DISK1\setup.msi" @(
	@copy "%build_dir%\installer\PROJECT_ASSISTANT\ADO.NET\DiskImages\DISK1\setup.msi" "%build_dir%\installer\HP_ADO.NET_%ADO_NET_Version%.msi"
)

@if "%ERRORLEVEL%" == "1" @(
	@echo Installer was built failed by InstallShield, please check privious errors.
	goto failedend
)
@REM RD /S /Q %build_dir%\installer\PROJECT_ASSISTANT

@echo on
@echo *** The installers are under %build_dir%\installer ***
@echo *** SUCCESS ***
@goto end

:failedend
@echo on
@echo ***  build failed ***

:end

