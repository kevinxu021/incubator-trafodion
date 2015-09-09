namespace Esgyndb.Data.ETL
{
    using System;

    internal class Header: INetworkReply
    {
        public int afRetCode;
        public int svcRetCode;

        public void ReadFromDataStream(DataStream ds, EsgyndbEncoder enc)
        {
            this.afRetCode = ds.ReadInt32();
            if (afRetCode != 0)
            {
                throw new Exception(string.Format("NeoGetInfo returned error code: {0}", afRetCode));
            }

            this.svcRetCode = ds.ReadInt32();
            if (svcRetCode != 0)
            {
                // TODO: read 3 ints + 1 string
                throw new Exception("DBTCLI_ERR_SVC_ERR");
            }
        }
    }
}
