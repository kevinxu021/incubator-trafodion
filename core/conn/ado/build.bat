@set build_dir=c:\Build
@set INNO_SETUP_PATH=C:\Program Files (x86)\Inno Setup 5

@set PATH=%INNO_SETUP_PATH%;%PATH%

@devenv EsgynDBProvider.sln /clean
@devenv EsgynDBProvider.sln /build "Release"

@set PATH=%INNO_SETUP_PATH%;%PATH%

if not exist "installer\Output\EsgynDB-ADO.NET-1.0.0.exe" goto failend

ISCC.exe /Q installer\installer.iss

@echo on
@echo *** build successfully *** goto end

:failend
@echo on
@echo *** build failend ***

:end