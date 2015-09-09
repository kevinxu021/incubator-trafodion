using System;
using System.Runtime.InteropServices;
using Microsoft.VisualStudio.Data;
using Microsoft.VisualStudio.Data.AdoDotNet;

namespace Esgyndb.Data.VisualStudio
{
    [Guid("113C3AC2-17B0-4099-9175-54E32292A5BF")]
    public sealed class EsgyndbProviderObjectFactory : AdoDotNetProviderObjectFactory
    {
        public override object CreateObject(Type objType)
        {
            if (objType == typeof(DataConnectionPromptDialog))
            {
                return new EsgyndbDataConnectionPromptDialog();
            }
            if (objType == typeof(DataConnectionProperties))
            {
                return new EsgyndbConnectionProperties();
            }
            if (objType == typeof(DataConnectionSupport))
            {
                return new EsgyndbConnectionSupport();
            }
            if (objType == typeof(DataConnectionUIControl))
            {
                return new EsgyndbConnectionUIControl();
            }
            if (objType == typeof(DataSourceSpecializer))
            {
                return new EsgyndbDataSourceSpecializer();
            }

            return base.CreateObject(objType);
        }
    }
}
