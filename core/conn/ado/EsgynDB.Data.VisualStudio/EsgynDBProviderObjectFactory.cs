using System;
using System.Runtime.InteropServices;
using Microsoft.VisualStudio.Data;
using Microsoft.VisualStudio.Data.AdoDotNet;

namespace EsgynDB.Data.VisualStudio
{
    [Guid("113C3AC2-17B0-4099-9175-54E32292A5BF")]
    public sealed class EsgynDBProviderObjectFactory : AdoDotNetProviderObjectFactory
    {
        public override object CreateObject(Type objType)
        {
            if (objType == typeof(DataConnectionPromptDialog))
            {
                return new EsgynDBDataConnectionPromptDialog();
            }
            if (objType == typeof(DataConnectionProperties))
            {
                return new EsgynDBConnectionProperties();
            }
            if (objType == typeof(DataConnectionSupport))
            {
                return new EsgynDBConnectionSupport();
            }
            if (objType == typeof(DataConnectionUIControl))
            {
                return new EsgynDBConnectionUIControl();
            }
            if (objType == typeof(DataSourceSpecializer))
            {
                return new EsgynDBDataSourceSpecializer();
            }

            return base.CreateObject(objType);
        }
    }
}
