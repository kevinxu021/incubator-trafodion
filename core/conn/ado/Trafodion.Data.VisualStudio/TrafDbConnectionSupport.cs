using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.Data.AdoDotNet;
using System.Windows.Forms;
using Microsoft.VisualStudio.Data;
using System.Data.Common;

namespace Trafodion.Data.VisualStudio
{
    internal class TrafDbConnectionSupport: AdoDotNetConnectionSupport
	{
        public TrafDbConnectionSupport(): 
            base("HP.Data")
        {
        }

        protected override object GetServiceImpl(Type serviceType)
        {
            if (serviceType == typeof(DataViewSupport))
            {
                return new TrafDbDataViewSupport();
            }
            if (serviceType == typeof(DataObjectSupport))
            {
                return new TrafDbDataObjectSupport();
            }
            if (serviceType == typeof(DataSourceInformation))
            {
                return new TrafDbDataSourceInformation(Site as DataConnection);
            }
         
            return base.GetServiceImpl(serviceType);
        }
    }
}
