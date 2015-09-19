@set build_dir=c:\Build

@devenv EsgynDBProvider.sln /clean
@devenv EsgynDBProvider.sln /build "Release"
@copy installer\installer\Express\SingleImage\DiskImages\DISK1\EsgynDB-ADO.NET-1.0.0.exe "%build_dir%"

if not exist "%build_dir%\EsgynDB-ADO.NET-1.0.0.exe" goto failend
@goto end

:failend
@echo on
@echo *** build failend ***

:end 
@echo on
@echo *** build successfully ***