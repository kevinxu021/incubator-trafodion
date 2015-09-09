using System;
using System.Reflection;
using Microsoft.VisualStudio.Data;

namespace Esgyndb.Data.VisualStudio
{
    class EsgyndbDataSourceSpecializer : DataSourceSpecializer
    {
        public override object CreateObject(Guid dataSource, Type objType)
        {
            return base.CreateObject(dataSource, objType);
        }

        public override Guid DeriveDataSource(string connectionString)
        {
            return base.DeriveDataSource(connectionString);
        }

        public override Assembly GetAssembly(Guid dataSource, string assemblyString)
        {
            return base.GetAssembly(dataSource, assemblyString);
        }
    }
}
