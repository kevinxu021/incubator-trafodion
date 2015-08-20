using System;
using System.Runtime.InteropServices;
using Microsoft.VisualStudio.Data;
using Microsoft.VisualStudio.Data.AdoDotNet;

namespace Trafodion.Data.VisualStudio
{
    [Guid("113C3AC2-17B0-4099-9175-54E32292A5BF")]
    public sealed class TrafDbProviderObjectFactory : AdoDotNetProviderObjectFactory
    {
        public override object CreateObject(Type objType)
        {
            if (objType == typeof(DataConnectionPromptDialog))
            {
                return new TrafDbDataConnectionPromptDialog();
            }
            if (objType == typeof(DataConnectionProperties))
            {
                return new TrafDbConnectionProperties();
            }
            if (objType == typeof(DataConnectionSupport))
            {
                return new TrafDbConnectionSupport();
            }
            if (objType == typeof(DataConnectionUIControl))
            {
                return new TrafDbConnectionUIControl();
            }
            if (objType == typeof(DataSourceSpecializer))
            {
                return new TrafDbDataSourceSpecializer();
            }

            return base.CreateObject(objType);
        }
    }
}
