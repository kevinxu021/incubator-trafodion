namespace Trafodion.Data.ETL
{
    internal class SegmentInformation : INetworkReply
    {
        public string Name;
        public int Number;
        public short SystemStatus;
        public short ProcessorStatus;

        public void ReadFromDataStream(DataStream ds, TrafDbEncoder enc)
        {
            this.Name = enc.GetString(ds.ReadString(), TrafDbEncoding.ISO88591);
            this.Number = ds.ReadInt32();
            this.SystemStatus = ds.ReadInt16();
            this.ProcessorStatus = ds.ReadInt16();
        }

        internal static SegmentInformation[] ReadListFromDataStream(DataStream ds, TrafDbEncoder enc)
        {
            int len = ds.ReadInt32();
            SegmentInformation[] segInfo = new SegmentInformation[len];
            for (int i = 0; i < len; i++)
            {
                segInfo[i] = new SegmentInformation();
                segInfo[i].ReadFromDataStream(ds, enc);
            }

            return segInfo;
        }
    }
}