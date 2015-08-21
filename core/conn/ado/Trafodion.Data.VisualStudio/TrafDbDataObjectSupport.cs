using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.Data;

namespace Trafodion.Data.VisualStudio
{
    internal class TrafDbDataObjectSupport : DataObjectSupport
    {
        public TrafDbDataObjectSupport()
            : base("HP.Data.VisualStudio.TrafDbDataObjectSupport", typeof(TrafDbDataObjectSupport).Assembly)
        {
        }
    }
}
