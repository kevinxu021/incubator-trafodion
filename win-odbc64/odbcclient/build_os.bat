
@set INNO_SETUP_PATH="C:\Program Files (x86)\Inno Setup 5"
@set PATH=%INNO_SETUP_PATH%;%PATH%

set MSBUILD_PATH=C:\Windows\Microsoft.NET\Framework64\v4.0.30319
REM get the build directory automatically, if failed please set this to top directory include everything for build
CD ..\..
SET BUILDDIR=%CD%
CD win-odbc64\odbcclient

REM set this to zlib header files directory
set ZLIB_INCLUDE_PATH=C:\zlib\include
REM set this to zlib library files directory
set ZLIB_LIB_PATH=C:\zlib\lib

REM set this to openssl header files directory
set OPENSSL_INCLUDE_PATH=C:\openssl-1.0.1e\include
REM set this to openssl library files directory
set OPENSSL_LIB_PATH=C:\openssl-1.0.1e\lib
set SRCDIR=%BUILDDIR%\win-odbc64
set LIBDIR=%BUILDDIR%\lib 
set PATH=%MSBUILD_PATH%\;%PATH%

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
msbuild.exe /t:rebuild Drvr35_os.vcxproj /p:Platform=x64 /p:Configuration=Release /p:OpenSSLIncludeDir=%OPENSSL_INCLUDE_PATH% /p:OpenSSLLibraryDir=%OPENSSL_LIB_PATH% /p:ZlibIncludeDir=%ZLIB_INCLUDE_PATH% /p:ZlibLibDir=%ZLIB_LIB_PATH%
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
msbuild.exe /t:rebuild TCPIPV4_os.vcxproj /p:Platform=x64 /p:Configuration=Release /p:ZlibIncludeDir=%ZLIB_INCLUDE_PATH% /p:ZlibLibDir=%ZLIB_LIB_PATH%
set BUILD_STATUS=%ERRORLEVEL%
if %BUILD_STATUS%==0 (
	echo Build TCPIPV4 success
) else ( 
	echo Build TCPIPV4 failed
	goto Exit
)

echo Building TCPIPV6 - Win64 Release...
cd %SRCDIR%\odbcclient\Drvr35\TCPIPV6
msbuild.exe /t:rebuild TCPIPV6_os.vcxproj /p:Platform=x64 /p:Configuration=Release /p:ZlibIncludeDir=%ZLIB_INCLUDE_PATH% /p:ZlibLibDir=%ZLIB_LIB_PATH%
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