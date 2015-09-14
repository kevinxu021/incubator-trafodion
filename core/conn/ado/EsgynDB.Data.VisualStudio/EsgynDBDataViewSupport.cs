using Microsoft.VisualStudio.Data;

namespace EsgynDB.Data.VisualStudio
{
    internal class EsgynDBDataViewSupport : DataViewSupport
    {
        public EsgynDBDataViewSupport()
            : base("EsgynDB.Data.VisualStudio.EsgyndbDataViewSupport", typeof(EsgynDBDataViewSupport).Assembly)
        {

        }
    }
}
