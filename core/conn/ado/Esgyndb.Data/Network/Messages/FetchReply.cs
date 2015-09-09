using System;

namespace Esgyndb.Data
{
    internal class FetchReply: INetworkReply
    {
        public ReturnCode returnCode;
        public SqlWarningOrError[] errorList;
        public long rowsAffected;
        public int dataFormat;
        public int dataOffset;

        public void ReadFromDataStream(DataStream ds, EsgyndbEncoder enc)
        {
            returnCode = (ReturnCode)ds.ReadInt32();

            if (returnCode != ReturnCode.Success && returnCode != ReturnCode.NoDataFound)
            {
                errorList = SqlWarningOrError.ReadListFromDataStream(ds, enc);
            }
            else
            {
                errorList = new SqlWarningOrError[0];
            }

            rowsAffected = ds.ReadUInt32();
            dataFormat = ds.ReadInt32();

            ds.ReadInt32(); // ignore length since we save entire buffer
            dataOffset = ds.Position;

            /*if (returnCode == ReturnCode.Success || returnCode == ReturnCode.SuccessWithInfo)
            {
                data = ds.ReadBytes();
            }*/
        }
    }
}
