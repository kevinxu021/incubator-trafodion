using Microsoft.VisualStudio.Data;

namespace Trafodion.Data.VisualStudio
{
    internal class TrafDbDataViewSupport : DataViewSupport
    {
        public TrafDbDataViewSupport()
            : base("HP.Data.VisualStudio.HPDbDataViewSupport", typeof(TrafDbDataViewSupport).Assembly)
        {

        }
    }
}
