using System;

namespace Trafodion.Data
{
    internal class InitDialogueMessage: INetworkMessage
    {
        public UserDescription UserDescription;
        public ConnectionContext ConnectionContext;

        public int DialogueId;
        public ConnectionContextOptions1 OptionFlags1;
        public uint OptionFlags2;
        public String SessionName;
        public String ClientUserName;

        private byte[] _sessionName;
        private byte[] _clientUserName;

        public void WriteToDataStream(DataStream ds)
        {
            UserDescription.WriteToDataStream(ds);
            ConnectionContext.WriteToDataStream(ds);
            
            ds.WriteInt32(DialogueId);
            ds.WriteUInt32((uint)OptionFlags1);
            ds.WriteUInt32(OptionFlags2);

            if ((OptionFlags1 & ConnectionContextOptions1.SessionName) > 0)
            {
                ds.WriteString(_sessionName);
            }

            if ((OptionFlags1 & ConnectionContextOptions1.ClientUsername) > 0)
            {
                ds.WriteString(_clientUserName);
            }
        }

        public int PrepareMessageParams(TrafDbEncoder enc) 
        {
            int len = 12; // 3*4 Int32

            len += UserDescription.PrepareMessageParams(enc);
            len += ConnectionContext.PrepareMessageParams(enc);

            if ((OptionFlags1 & ConnectionContextOptions1.SessionName) > 0)
            {
                len += 4;

                _sessionName = enc.GetBytes(SessionName, enc.Transport);
                if (_sessionName.Length > 0)
                {
                    len += _sessionName.Length + 1;
                }
            }

            if ((OptionFlags1 & ConnectionContextOptions1.ClientUsername) > 0)
            {
                len += 4;

                _clientUserName = enc.GetBytes(ClientUserName, enc.Transport);
                if (_clientUserName.Length > 0)
                {
                    len += _clientUserName.Length + 1;
                }
            }

            return len;
        }
    }
}
