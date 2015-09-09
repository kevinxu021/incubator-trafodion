using System;
namespace Esgyndb.Data.ETL
{
    internal class QueryInformation: INetworkReply
    {
        public string QueryText;
        public int SegmentHint;
        public byte CpuHint;

        public void ReadFromDataStream(DataStream ds, EsgyndbEncoder enc)
        {
            // we wont use this specifically 
            // just read them as a group since the params are not grouped for individual reads
            throw new System.NotImplementedException();
        }

        internal static QueryInformation[] ReadListFromDataStream(DataStream ds, EsgyndbEncoder enc, int len)
        {
            QueryInformation[] queryInfo = new QueryInformation[len];

            for (int i = 0; i < len; i++)
            {
                queryInfo[i] = new QueryInformation();
                ds.ReadInt32(); // unused charset
                queryInfo[i].QueryText = enc.GetString(ds.ReadString(),enc.Transport);
            }

            byte[] b = new byte[4];
            for (int i = 0; i < len; i++)
            {
                uint temp = ds.ReadUInt32();

                // TODO: this might break on Seaquest with the endianess changes, not sure how 
                // this is packed on the server side
                queryInfo[i].SegmentHint = (int)(temp >> 8);
                queryInfo[i].CpuHint = (byte)(temp & 0xFF);
            }

            return queryInfo;
        }
    }
}