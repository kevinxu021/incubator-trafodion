using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.Data.AdoDotNet;
using System.Windows.Forms;
using Microsoft.VisualStudio.Data;
using System.Data.Common;

namespace EsgynDB.Data.VisualStudio
{
    internal class EsgynDBConnectionSupport: AdoDotNetConnectionSupport
	{
        public EsgynDBConnectionSupport(): 
            base("EsgynDB.Data")
        {
        }

        protected override object GetServiceImpl(Type serviceType)
        {
            if (serviceType == typeof(DataViewSupport))
            {
                return new EsgynDBDataViewSupport();
            }
            if (serviceType == typeof(DataObjectSupport))
            {
                return new EsgynDBDataObjectSupport();
            }
            if (serviceType == typeof(DataSourceInformation))
            {
                return new EsgynDBDataSourceInformation(Site as DataConnection);
            }
         
            return base.GetServiceImpl(serviceType);
        }
    }
}
