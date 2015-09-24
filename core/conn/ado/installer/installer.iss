; Script generated by the Inno Script Studio Wizard.
; SEE THE DOCUMENTATION FOR DETAILS ON CREATING INNO SETUP SCRIPT FILES!

#define MyAppName "EsgynDB ADO.NET"
#define MyAppVersion "1.0.0"
#define MyAppPublisher "Esgyn Corporation"
#define MyAppURL "http://www.esgyn.com/"

[Setup]
; NOTE: The value of AppId uniquely identifies this application.
; Do not use the same AppId value in installers for other applications.
; (To generate a new GUID, click Tools | Generate GUID inside the IDE.)
AppId={{6DBEB17E-258A-448C-BAAE-1EEA836BC002}
AppName={#MyAppName} {#MyAppVersion}
AppVersion={#MyAppVersion}
;AppVerName={#MyAppName} {#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={pf}\EsgynDB\ADO.NET-1.0.0
LicenseFile=Eula.rtf
Compression=lzma
SolidCompression=yes
AppCopyright=Copyright 2015 Esgyn Corporation
UserInfoPage=True
ArchitecturesInstallIn64BitMode=x64
UninstallDisplayName={#MyAppName} {#MyAppVersion}
OutputBaseFilename=EsgynDB-ADO.NET-{#MyAppVersion}
WizardImageBackColor=$00333333
WizardImageStretch=False
UsePreviousGroup=False
DisableProgramGroupPage=yes

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Files]
Source: "C:\Build\EsgynDBADO\Release\EsgynDB.Data.dll"; DestDir: "{app}"; Flags: ignoreversion
; NOTE: Don't use "Flags: ignoreversion" on any shared system files

[Icons]
Name: "{group}\{cm:UninstallProgram,{#MyAppName} {#MyAppVersion}}"; Filename: "{uninstallexe}"

[Registry]
Root: "HKLM"; Subkey: "SOFTWARE\Microsoft\.NETFramework\v2.0.50727\AssemblyFoldersEx\EsgynDB ADO.NET 1.0.0"; ValueData: "{app}"
Root: "HKLM"; Subkey: "SOFTWARE\Microsoft\.NETFramework\AssemblyFolders\EsgynDB ADO.NET 1.0.0"; ValueData: "{app}"
Root: "HKLM"; Subkey: "SOFTWARE\Microsoft\.NETFramework\v4.0.30319\AssemblyFoldersEx\EsgynDB ADO.NET 1.0.0"; ValueData: "{app}"
