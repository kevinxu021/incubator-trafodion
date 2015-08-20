using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.Data;

namespace Trafodion.Data.VisualStudio
{
    internal class HPDbDataObjectSupport : DataObjectSupport
    {
        public HPDbDataObjectSupport()
            : base("HP.Data.VisualStudio.HPDbDataObjectSupport", typeof(HPDbDataObjectSupport).Assembly)
        {
        }
    }
}
