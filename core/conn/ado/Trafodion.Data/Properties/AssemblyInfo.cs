using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Trafodion.Data;

// General Information about an assembly is controlled through the following 
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: AssemblyTitle("Trafodion.Data")]
[assembly: AssemblyDescription(ProductVersion.Vproc)]
[assembly: AssemblyConfiguration("Release")]
[assembly: AssemblyCompany("Esgyn Company")]
[assembly: AssemblyProduct("Esgyn ADO.NET 1.0 Provider")]
[assembly: AssemblyCopyright("Copyright © Esgyn Company 2015")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

// Setting ComVisible to false makes the types in this assembly not visible 
// to COM components.  If you need to access a type in this assembly from 
// COM, set the ComVisible attribute to true on that type.
[assembly: ComVisible(false)]

// The following GUID is for the ID of the typelib if this project is exposed to COM
//[assembly: Guid("12122797-6bbc-4741-96f3-bef0437b0bcd")]
[assembly: Guid("21FE9FCD-0F2E-4CB6-8422-C6E52464E801")]

// Version information for an assembly consists of the following four values:
//
//      Major Version
//      Minor Version 
//      Build Number
//      Revision
//
// You can specify all the values or you can default the Build and Revision Numbers 
// by using the '*' as shown below:
// [assembly: AssemblyVersion("1.0.*")]
[assembly: AssemblyVersion(DisplayVersion.VersionStr)]
[assembly: AssemblyFileVersion(DisplayVersion.FileVersion)]
[assembly: AssemblyInformationalVersion(DisplayVersion.VersionStr)]
[assembly: InternalsVisibleTo("Trafodion.Data.ETL")]
