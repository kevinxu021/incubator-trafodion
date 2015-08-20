using System;

namespace Trafodion.Data
{
    internal class CancelMessage: INetworkMessage
    {
        public int DialogueId;
        public int ServerType;
        public string ServerObjRef;
        public int StopType;

        private byte[] _serverObjRef;

        public void WriteToDataStream(DataStream ds)
        {
            ds.WriteInt32(DialogueId);
            ds.WriteInt32(ServerType);
            ds.WriteString(_serverObjRef);
            ds.WriteInt32(StopType);
        }

        public int PrepareMessageParams(TrafDbEncoder enc)
        {
            int len = 16; //3*4 Int32, 1*4 string len

            _serverObjRef = enc.GetBytes(ServerObjRef, enc.Transport);
            if (_serverObjRef.Length > 0)
            {
                len += _serverObjRef.Length + 1;
            }

            return len;
        }
    }
}
