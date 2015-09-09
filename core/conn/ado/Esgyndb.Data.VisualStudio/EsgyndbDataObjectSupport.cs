using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.Data;

namespace Esgyndb.Data.VisualStudio
{
    internal class EsgyndbDataObjectSupport : DataObjectSupport
    {
        public EsgyndbDataObjectSupport()
            : base("Esgyndb.Data.VisualStudio.EsgyndbDataObjectSupport", typeof(EsgyndbDataObjectSupport).Assembly)
        {
        }
    }
}
