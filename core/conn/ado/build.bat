@echo off

rem *********************************************************************
rem @@@ START COPYRIGHT @@@
rem
rem        HP CONFIDENTIAL: NEED TO KNOW ONLY
rem
rem        Copyright 2002
rem        Hewlett-Packard Development Company, L.P.
rem        Protected as an unpublished work.
rem
rem  The computer program listings, specifications and documentation 
rem  herein are the property of Hewlett-Packard Development Company,
rem  L.P., or a third party supplier and shall not be reproduced, 
rem  copied, disclosed, or used in whole or in part for any reason 
rem  without the prior express written permission of Hewlett-Packard 
rem  Development Company, L.P.
rem
rem @@@ END COPYRIGHT @@@ 
rem ********************************************************************/

rem @@@ START COPYRIGHT @@@
rem
rem  Copyright 1998
rem  Compaq Computer Corporation
rem     Protected as an unpublished work.
rem        All rights reserved.
rem
rem  The computer program listings, specifications, and documentation
rem  herein are the property of Compaq Computer Corporation or a
rem  third party supplier and shall not be reproduced, copied,
rem  disclosed, or used in whole or in part for any reason without
rem  the prior express written permission of Compaq Computer
rem  Corporation.
rem
rem @@@ END COPYRIGHT @@@

REM 
call "%VS90COMNTOOLS%vsvars32.bat"

if "%1"=="help" goto help
if {%1}=={} goto recevier

REM varify sln file
if not exist "NeoviewProvider.sln" goto unexist
if not exist "build.proj" goto unexist
else goto build



:help
echo.
echo Hewlett-Packard Development Company, L.P.
echo use: 
echo 		bulder.bat [Location Path]
goto end



:recevier
echo.
echo Please input the installation path:
set /p install_path=
echo Path is [%install_path%], is it correct?
set /p par=Yes or No:
if "%par%" == "yes" goto build
if not "%par%"=="yes" goto recevier


:build
perl .\update_version.pl
mkdir %install_path%
echo.
echo ++ Build Started now ++
echo.
rem msbuild "NeoviewProvider.sln"
rem msbuild build.proj /p:outputpath=%install_path%
devenv NeoviewProvider.sln /Build debug /Project Deploy
echo.
echo ++ Build Successed ++
REM echo copy the .msi into target folder
REM need to be replaced with better way
REM VS donot provide a way to config vdproj
REM copy ..\..\..\..\..\..\Build\HPAdo\*.msi %install_path%
goto end



:unexist
echo "NeoviewProvider.sln" doesnot exist, Please make sure .BAT file is located under the same path with .sln
goto end



:error
echo Error occured
goto end



:end
echo.
pause

