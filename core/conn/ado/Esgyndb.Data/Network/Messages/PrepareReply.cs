using System;

namespace Esgyndb.Data
{
    internal class PrepareReply: INetworkReply
    {
        public ReturnCode returnCode;
        public SqlWarningOrError[] errorList;
        public QueryType queryType;
        public int stmtHandle;
        public int estimatedCost;

        public int inputDescLength;
        public int inputRowLength;
        public Descriptor[] inputDesc;

        public int outputDescLength;
        public int outputRowLength;
        public Descriptor[] outputDesc;

        public void ReadFromDataStream(DataStream ds, EsgyndbEncoder enc)
        {
            returnCode = (ReturnCode)ds.ReadInt32();

            if (returnCode != ReturnCode.Success)
            {
                errorList = SqlWarningOrError.ReadListFromDataStream(ds, enc);
            }

            if (returnCode == ReturnCode.Success|| returnCode == ReturnCode.SuccessWithInfo)
            {
                queryType = (QueryType) ds.ReadInt32();
                stmtHandle = ds.ReadInt32();
                estimatedCost = ds.ReadInt32();

                inputDescLength = ds.ReadInt32();
                if (inputDescLength > 0)
                {
                    inputRowLength = ds.ReadInt32();
                    inputDesc = Descriptor.ReadListFromDataStream(ds, enc);
                }
                else
                {
                    inputDesc = new Descriptor[0];
                }

                outputDescLength = ds.ReadInt32();
                if (outputDescLength > 0)
                {
                    outputRowLength = ds.ReadInt32();
                    outputDesc = Descriptor.ReadListFromDataStream(ds, enc);
                }
                else
                {
                    outputDesc = new Descriptor[0];
                }
            }
        }
    }
}
