using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Esgyndb.Data
{
    /*internal enum MetaDataError : int
    {
        Success = 0,
        ParamError = 1,
        InvalidConnection = 2,
        SqlError = 3,
        InvalidHandle = 4,
    }

    internal class MetaDataReply: INetworkReply
    {
        public MetaDataError Error;
        public int ErrorDetail;
        public ErrorDesc[] ErrorDescList;
        public string ErrorText;

        public string p2;
        public Descriptor[] Descriptors;
        public string ProxySyntax;

        public void ReadFromDataStream(DataStream ds)
        {
            Error = (MetaDataError) ds.ReadInt32();
            ErrorDetail = ds.ReadInt32();

            switch (Error)
            {
                case MetaDataError.SqlError:
                    ErrorDescList = ErrorDesc.ReadListFromDataStream(ds);
                    break;
                case MetaDataError.ParamError:
                    ErrorText = ds.ReadString();
                    break;
                case MetaDataError.Success:
                    p2 = ds.ReadString();
                    Descriptors = Descriptor.ReadListFromDataStream(ds);
                    ErrorDescList = ErrorDesc.ReadListFromDataStream(ds);
                    ProxySyntax = ds.ReadString();
                    break;
            }
        }
    }*/
}
