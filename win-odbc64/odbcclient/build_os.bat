@echo off
REM @@@ START COPYRIGHT @@@
REM
REM
REM @@@ END COPYRIGHT @@@

REM set this to openssl binary directory.
set OPENSSL_PATH=C:\openssl-1.0.1e
REM set ODBCCP32_64_PATH="C:\Program Files (x86)\Microsoft Visual Studio 8\VC\PlatformSDK\Lib\amd64"
set MSBUILD_PATH=C:\Windows\Microsoft.NET\Framework64\v4.0.30319
REM set this to top directory include everything for build
set BUILDDIR=

set ZLIB_SRC=%BUILDDIR%\zlib_official
set ZLIB_SRC_GIT=%ZLIB_SRC:\=/%

set SRCDIR=%BUILDDIR%\win-odbc64
set LIBDIR=%BUILDDIR%\lib 
set PATH=%MSBUILD_PATH%\;%PATH%
set DATE=
set ALL_SUCCESS=0

echo=
echo ===============================
echo     BUILD WIN64 RELEASE
echo ===============================
echo=

echo Building Drvr35Msg - Win64 Release...
cd %SRCDIR%\odbcclient\Drvr35Msg
msbuild.exe /t:rebuild Drvr35Msg_os.vcxproj /p:Platform=x64 /p:Configuration=Release
set BUILD_STATUS=%ERRORLEVEL%
if %BUILD_STATUS%==0 (
	echo Build Drvr35Msg success
) else (
	echo Build Drvr35Msg failed
	goto Exit
)


echo Building Drvr35 - Win64 Release...
cd %SRCDIR%\odbcclient\Drvr35
msbuild.exe /t:rebuild Drvr35_os.vcxproj /p:Platform=x64 /p:Configuration=Release /p:OpenSSLIncludeDir=%OPENSSL_PATH%\include /p:OpenSSLLibraryDir=%OPENSSL_PATH%\lib\VC\static 
set BUILD_STATUS=%ERRORLEVEL%
if %BUILD_STATUS%==0 (
	echo Build Drvr35 success
) else ( 
	echo Build Drvr35 failed
	goto Exit
)
  
echo Building Drvr35Adm - Win64 Release...
cd %SRCDIR%\odbcclient\Drvr35Adm
msbuild.exe /t:rebuild Drvr35Adm_os.vcxproj /p:Platform=x64 /p:Configuration=Release
set BUILD_STATUS=%ERRORLEVEL%
if %BUILD_STATUS%==0 (
	echo Build Drvr35Adm success
) else ( 
	echo Build Drvr35Adm failed
	goto Exit
)

REM echo Building Drvr35Trace - Win64 Release...
REM cd %SRCDIR%\odbcclient\Drvr35Trace
REM msbuild.exe /t:rebuild Drvr35Trace_os.vcxproj /p:Platform=x64 /p:Configuration=Release
REM set BUILD_STATUS=%ERRORLEVEL%
REM if %BUILD_STATUS%==0 (
REM	echo Build Drvr35Trace success
REM ) else ( 
REM	echo Build Drvr35Trace failed
REM	goto Exit
REM )

echo Building TCPIPV4 - Win64 Release...
cd %SRCDIR%\odbcclient\Drvr35\TCPIPV4
msbuild.exe /t:rebuild TCPIPV4_os.vcxproj /p:Platform=x64 /p:Configuration=Release
set BUILD_STATUS=%ERRORLEVEL%
if %BUILD_STATUS%==0 (
	echo Build TCPIPV4 success
) else ( 
	echo Build TCPIPV4 failed
	goto Exit
)

echo Building TCPIPV6 - Win64 Release...
cd %SRCDIR%\odbcclient\Drvr35\TCPIPV6
msbuild.exe /t:rebuild TCPIPV6_os.vcxproj /p:Platform=x64 /p:Configuration=Release
set BUILD_STATUS=%ERRORLEVEL%
if %BUILD_STATUS%==0 (
	echo Build TCPIPV6 success
) else ( 
	echo Build TCPIPV6 failed
	goto Exit
)

echo Building TranslationDll - Win64 Release...
cd %SRCDIR%\odbcclient\TranslationDll
msbuild.exe /t:rebuild TranslationDll_os.vcxproj /p:Platform=x64 /p:Configuration=Release	
set BUILD_STATUS=%ERRORLEVEL%
if %BUILD_STATUS%==0 (
	echo Build TranslationDll success
) else ( 
	echo Build TranslationDll failed
	goto Exit
)

echo Building Drvr35Res - Win64 Release...
cd %SRCDIR%\odbcclient\Drvr35Res 
msbuild.exe /t:rebuild Drvr35Res_os.vcxproj /p:Platform=x64 /p:Configuration=Release	
set BUILD_STATUS=%ERRORLEVEL%
if %BUILD_STATUS%==0 (
	echo Build Drvr35Res success
) else ( 
	echo Build Drvr35Res failed
	goto Exit
)
   
echo Building SetCertificateDirReg InstallHelper - Win64 Release...
cd %SRCDIR%\Install\SetCertificateDirReg\SetCertificateDirReg
msbuild.exe /t:rebuild SetCertificateDirReg_os.vcxproj /p:Platform=x64 /p:Configuration=Release /p:Odbccp32lib64Dir=%ODBCCP32_64_PATH% 
set BUILD_STATUS=%ERRORLEVEL%
if %BUILD_STATUS%==0 (
	echo Build SetCertificateDirReg success
) else ( 
	echo Build SetCertificateDirReg failed
	goto Exit
)
 
echo Building UpdateDSN InstallHelper - Win64 Release...
cd %SRCDIR%\Install\UpdateDSN\UpdateDSN
msbuild.exe /t:rebuild UpdateDSN_os.vcxproj /p:Platform=x64 /p:Configuration=Release
set BUILD_STATUS=%ERRORLEVEL%
if %BUILD_STATUS%==0 (
	echo Build UpdateDSN success
) else ( 
	echo Build UpdateDSN failed
	goto Exit
)
set ALL_SUCCESS=1

:Exit
if %ALL_SUCCESS%==1 (
	echo=
	echo ========================================
	echo     BUILD WIN64 RELEASE SUCCESSFULLY
	echo ========================================
	echo=
)
cd %SRCDIR%\odbcclient
@echo on
