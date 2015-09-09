namespace Esgyndb.Data.ETL
{
    internal class GetParallelExtractInfoReply: INetworkReply
    {
        private int _len;

        public Header Header;
        public QueryInformation[] QueryInfo;
        
        // the length is not returned in the network message so we need it up front
        public GetParallelExtractInfoReply(int len)
        {
            this._len = len;
        }

        public void ReadFromDataStream(DataStream ds, EsgyndbEncoder enc)
        {
            Header = new Header();
            Header.ReadFromDataStream(ds, enc);

            QueryInfo = QueryInformation.ReadListFromDataStream(ds, enc, this._len);
        }
    }
}
