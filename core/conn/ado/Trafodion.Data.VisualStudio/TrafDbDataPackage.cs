using System;
using System.ComponentModel.Design;
using System.Runtime.InteropServices;
using Microsoft.VisualStudio.Shell;

namespace Trafodion.Data.VisualStudio
{
    [ProvideLoadKey("Standard", "3.0.0.0", "Trafodion ADO.NET 3.0 Package", "Trafodion", 100)]
    [Guid("F60A05D1-9D3B-42F3-B7E3-3D4924399FAB")]
    [DefaultRegistryRoot(@"Software\Microsoft\VisualStudio\9.0")]
    [PackageRegistration(UseManagedResourcesOnly = true)]
    [ProvideService(typeof(TrafDbProviderObjectFactory))]
    [ProvideAutoLoad("{f1536ef8-92ec-443c-9ed7-fdadf150da82}")]
    [InstalledProductRegistration(false, "#101", "#102", "#103", IconResourceID = 200)]
    public sealed class TrafDbDataPackage : Package
    {
        protected override void Initialize()
        {
            base.Initialize();

            ((IServiceContainer)this).AddService(typeof(TrafDbProviderObjectFactory),
                    new ServiceCreatorCallback(this.Callback), true);
        }

        public object Callback(IServiceContainer sc, Type t)
        {
            return (t == typeof(TrafDbProviderObjectFactory))?new TrafDbProviderObjectFactory():null;
        }
    }
}
