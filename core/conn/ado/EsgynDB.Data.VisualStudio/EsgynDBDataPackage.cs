using System;
using System.ComponentModel.Design;
using System.Runtime.InteropServices;
using Microsoft.VisualStudio.Shell;

namespace EsgynDB.Data.VisualStudio
{
    [ProvideLoadKey("Standard", "1.0.0.0", "EsgynDB ADO.NET 1.0 Package", "EsgynDB", 100)]
    [Guid("F60A05D1-9D3B-42F3-B7E3-3D4924399FAB")]
    [DefaultRegistryRoot(@"Software\Microsoft\VisualStudio\9.0")]
    [PackageRegistration(UseManagedResourcesOnly = true)]
    [ProvideService(typeof(EsgynDBProviderObjectFactory))]
    [ProvideAutoLoad("{f1536ef8-92ec-443c-9ed7-fdadf150da82}")]
    [InstalledProductRegistration(false, "#101", "#102", "#103", IconResourceID = 200)]
    public sealed class EsgynDBDataPackage : Package
    {
        protected override void Initialize()
        {
            base.Initialize();

            ((IServiceContainer)this).AddService(typeof(EsgynDBProviderObjectFactory),
                    new ServiceCreatorCallback(this.Callback), true);
        }

        public object Callback(IServiceContainer sc, Type t)
        {
            return (t == typeof(EsgynDBProviderObjectFactory))?new EsgynDBProviderObjectFactory():null;
        }
    }
}
