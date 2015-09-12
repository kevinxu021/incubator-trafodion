using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.Data;

namespace EsgynDB.Data.VisualStudio
{
    internal class EsgynDBDataObjectSupport : DataObjectSupport
    {
        public EsgynDBDataObjectSupport()
            : base("EsgynDB.Data.VisualStudio.EsgyndbDataObjectSupport", typeof(EsgynDBDataObjectSupport).Assembly)
        {
        }
    }
}
