@set build_dir=c:\Build\ado
@set INNO_SETUP_PATH=d:\Program Files (x86)\Inno Setup 5
@set DEVENV_HOME=c:\Program Files (x86)\Microsoft Visual Studio 12.0\Common7\IDE
@set TrafVersion=2.0.0
@set TrafCopyright="Copyright 2015 Esgyn Corporation"
@set curr_path=%~dp0

@if not exist "%INNO_SETUP_PATH%" (
  echo Please check your configured INNO_SETUP_PATH which doesnot exist.
  goto failend
) else (
  @if not exist "%DEVENV_HOME%" (
    echo Please check your configured DEVENV_HOME which doesnot exist.
    goto failend
  )
)
@if not exist "%build_dir%" (
  md "%build_dir%"
  echo "create directory %build_dir%"
)
@if exist "%build_dir%\EsgynDB-ADO.NET-%TrafVersion%.exe" ( 
  del /F /S /Q "%build_dir%\EsgynDB-ADO.NET-%TrafVersion%.exe"
)
@set PATH=%INNO_SETUP_PATH%;%DEVENV_HOME%;%PATH%

@devenv.exe "%curr_path%\EsgynDBProvider.sln" /rebuild "Release" /Project EsgynDB.Data

@if not exist "%curr_path%\EsgynDB.Data\Release\EsgynDB.Data.dll" ( 
  echo  VS build failed. File "%curr_path%\EsgynDB.Data\Release\EsgynDB.Data.dll" doesn't exist.
  goto failend
)
@REM @copy "%curr_path%\EsgynDB.Data\Release\*" %build_dir%

@ISCC.exe /DTrafVersion=%TrafVersion% /DCurrentPath=%curr_path% /DTrafCopyright=%TrafCopyright% /O"%build_dir%" /Q "%curr_path%\installer\installer.iss"
@if not exist "%build_dir%\EsgynDB-ADO.NET-%TrafVersion%.exe" goto failend
@echo File generated: %build_dir%\EsgynDB-ADO.NET-%TrafVersion%.exe

@echo on
@echo *** build successfully *** 
@goto end

:failend
@echo on
@echo *** build failend ***

:end