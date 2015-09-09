namespace Esgyndb.Data.ETL
{
    using System;

    internal class GetNeoInfoReply: INetworkReply
    {
        public Header Header;
        public SegmentInformation[] SegmentInfo;

        public string TrafSchemaVersion;
        public string TransporterSchemaVersion;
        public string TransporterVersion;

        public void ReadFromDataStream(DataStream ds, EsgyndbEncoder enc)
        {
            Header = new Header();
            Header.ReadFromDataStream(ds, enc);

            this.SegmentInfo = SegmentInformation.ReadListFromDataStream(ds, enc);

            this.TrafSchemaVersion = enc.GetString(ds.ReadString(), EsgyndbEncoding.ISO88591);
            this.TransporterSchemaVersion = enc.GetString(ds.ReadString(), EsgyndbEncoding.ISO88591);
            this.TransporterVersion = enc.GetString(ds.ReadString(), EsgyndbEncoding.ISO88591);
        }
    }
}