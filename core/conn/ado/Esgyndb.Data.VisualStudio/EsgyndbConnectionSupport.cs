using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.Data.AdoDotNet;
using System.Windows.Forms;
using Microsoft.VisualStudio.Data;
using System.Data.Common;

namespace Esgyndb.Data.VisualStudio
{
    internal class EsgyndbConnectionSupport: AdoDotNetConnectionSupport
	{
        public EsgyndbConnectionSupport(): 
            base("Esgyndb.Data")
        {
        }

        protected override object GetServiceImpl(Type serviceType)
        {
            if (serviceType == typeof(DataViewSupport))
            {
                return new EsgyndbDataViewSupport();
            }
            if (serviceType == typeof(DataObjectSupport))
            {
                return new EsgyndbDataObjectSupport();
            }
            if (serviceType == typeof(DataSourceInformation))
            {
                return new EsgyndbDataSourceInformation(Site as DataConnection);
            }
         
            return base.GetServiceImpl(serviceType);
        }
    }
}
