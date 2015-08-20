using System;

namespace Trafodion.Data
{
    internal class GetObjRefMessage: INetworkMessage
    {
        public ConnectionContext ConnectionContext;
        public UserDescription UserDescription;

        public int ServerType;
        public short RetryCount;

        public string ClientUsername;

        private byte[] _clientUsername;

        public void WriteToDataStream(DataStream ds)
        {
            ConnectionContext.WriteToDataStream(ds);
            UserDescription.WriteToDataStream(ds);

            ds.WriteInt32(ServerType);
            ds.WriteInt16(RetryCount);

            //TODO: splitting up how the struct is written is messy
            ds.WriteUInt32((uint)ConnectionContext.InContextOptions1);
            ds.WriteUInt32(ConnectionContext.InContextOptions2);
            ds.WriteString(ConnectionContext._clientVproc);

            ds.WriteString(_clientUsername);
        }

        public int PrepareMessageParams(TrafDbEncoder enc)
        {
            int len = 22; //5*4 Int32, 1*2 Int16

            len += ConnectionContext.PrepareMessageParams(enc);
            len += UserDescription.PrepareMessageParams(enc);

            if (ConnectionContext._clientVproc.Length > 0)
            {
                len += ConnectionContext._clientVproc.Length + 1;
            }

            _clientUsername = enc.GetBytes(ClientUsername, enc.Transport);
            if (_clientUsername.Length > 0)
            {
                len += _clientUsername.Length + 1;
            }

            return len;
        }
    }
}
