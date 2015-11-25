; Script generated by the Inno Script Studio Wizard.
; SEE THE DOCUMENTATION FOR DETAILS ON CREATING INNO SETUP SCRIPT FILES!

#define MyAppName "EsgynDB ADO.NET"
#define MyAppPublisher "Esgyn Corporation"
#define MyAppURL "http://www.esgyn.com/"

[Setup]
; NOTE: The value of AppId uniquely identifies this application.
; Do not use the same AppId value in installers for other applications.
; (To generate a new GUID, click Tools | Generate GUID inside the IDE.)
AppId={{6DBEB17E-258A-448C-BAAE-1EEA836BC002}
AppName={#MyAppName} {#TrafVersion}
AppVersion={#TrafVersion}
AppVerName={#MyAppName} {#TrafVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={pf}\EsgynDB\ADO.NET-{#TrafVersion}
DefaultGroupName={#MyAppName}
LicenseFile=Eula.rtf
Compression=lzma
SolidCompression=yes
AppCopyright=Copyright 2015 Esgyn Corporation
UserInfoPage=True
ArchitecturesInstallIn64BitMode=x64
UninstallDisplayName={#MyAppName} {#TrafVersion}
OutputBaseFilename=EsgynDB-ADO.NET-{#TrafVersion}
WizardImageBackColor=$00333333
WizardImageStretch=False
UsePreviousGroup=False
DisableProgramGroupPage=yes
SetupLogging=yes

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Files]
Source: "..\EsgynDB.Data\Release\EsgynDB.Data.dll"; DestDir: "{app}"; StrongAssemblyName: "EsgynDB.Data, Version={#TrafVersion}.0, PublicKeyToken=cfb872a824fb4c13, Culture=neutral, ProcessorArchitecture=MSIL";Flags: "gacinstall sharedfile uninsnosharedfileprompt"

; NOTE: Don't use "Flags: ignoreversion" on any shared system files

[Icons]
Name: "{group}\{cm:UninstallProgram,{#MyAppName} {#TrafVersion}}"; Filename: "{uninstallexe}"

[Registry]
Root: "HKLM"; Subkey: "SOFTWARE\Microsoft\.NETFramework\v2.0.50727\AssemblyFoldersEx\EsgynDB ADO.NET {#TrafVersion}"; ValueType: string; ValueData: "{app}" ;Flags: uninsdeletekey
Root: "HKLM"; Subkey: "SOFTWARE\Microsoft\.NETFramework\AssemblyFolders\EsgynDB ADO.NET {#TrafVersion}"; ValueType: string; ValueData: "{app}" ;Flags: uninsdeletekey
Root: "HKLM"; Subkey: "SOFTWARE\Microsoft\.NETFramework\v4.0.30319\AssemblyFoldersEx\EsgynDB ADO.NET {#TrafVersion}"; ValueType: string;  ValueData: "{app}" ;Flags: uninsdeletekey

[Code]
function GetUninstallString: string;
var
  sUnInstPath: string;
  sUnInstallString: String;
  sAppId: String;
begin
  Result := '';
  sAppId := ExpandConstant('{#emit SetupSetting("AppId")}_is1');
  sUnInstPath := ExpandConstant('Software\Microsoft\Windows\CurrentVersion\Uninstall\') + sAppId;
  sUnInstallString := '';
  if not RegQueryStringValue(HKLM, sUnInstPath, 'UninstallString', sUnInstallString) then
    RegQueryStringValue(HKCU, sUnInstPath, 'UninstallString', sUnInstallString);
  Result := sUnInstallString;
end;

function IsUpgrade: Boolean;
begin
  Result := (GetUninstallString() <> '');
end;

function InitializeSetup: Boolean;
var
  V: Integer;
  iResultCode: Integer;
  sUnInstallString: string;
  sAppId: String;
  sAppName: String;
begin
  Result := True; // in case when no previous version is found
  sAppId := ExpandConstant('{#emit SetupSetting("AppId")}_is1');
  sAppName := ExpandConstant('{#emit SetupSetting("AppVerName")}');
  if RegValueExists(HKEY_LOCAL_MACHINE,'Software\Microsoft\Windows\CurrentVersion\Uninstall\' + sAppId, 'QuietUninstallString') then  //Your App GUID/ID
  begin
    V := MsgBox(ExpandConstant('An existing install of '+ sAppName + ' was detected.' + #13#10#13#10 +'The installer will now uninstall the old version and install the new version.' + #13#10#13#10 +'Do you want to continue?'), mbInformation, MB_YESNO); //Custom Message if App installed
    if V = IDYES then
    begin
      sUnInstallString := GetUninstallString();
      sUnInstallString :=  RemoveQuotes(sUnInstallString);
      Exec(ExpandConstant(sUnInstallString), '/SILENT', '', SW_SHOW, ewWaitUntilTerminated, iResultCode);
      Result := True; //if you want to proceed after uninstall
                //Exit; //if you want to quit after uninstall
    end
    else
      Result := False; //when older version present and not uninstalled
  end;
end;