using System;
using System.Net;
using System.Net.Sockets;

namespace EsgynDB.Data
{
    internal class GetObjRefMessage: INetworkMessage
    {
        public ConnectionContext ConnectionContext;
        public UserDescription UserDescription;

        public int ServerType;
        public short RetryCount;

        public string ClientUsername;

        private byte[] _clientUsername;
        private EsgynDBEncoder encoder;

        public void WriteToDataStream(DataStream ds)
        {
            IPHostEntry heserver = Dns.GetHostEntry(ConnectionContext.ComputerName);
            String ipClientAddress = "";

            IPAddress[] ipv4Addresses = Array.FindAll(heserver.AddressList,
                item => item.AddressFamily == AddressFamily.InterNetwork);

            foreach (IPAddress ipAddress in ipv4Addresses)
            {
                String ipString = ipAddress.ToString();
                if (!ipString.Equals(ipClientAddress))
                {
                    ipClientAddress = ipString;
                }
            }

            String roleName = ConnectionContext.UserRole;
            String ccExtention = String.Format("{{\"sessionName\":\"{0}\",\"ipClientAddress\":\"{1}\",\"clientHostName\":\"{2}\",\"userName\":\"{3}\",\"roleName\":\"{4}\",\"applicationName\":\"{5}\"}}",
                       ConnectionContext.SessionName,
                       ipClientAddress,
                       ConnectionContext.ComputerName,
                       ClientUsername,
                       roleName,
                       System.AppDomain.CurrentDomain.FriendlyName;);

            ConnectionContext.WriteToDataStream(ds);
            UserDescription.WriteToDataStream(ds);

            ds.WriteInt32(ServerType);
            ds.WriteInt16(RetryCount);

            //TODO: splitting up how the struct is written is messy
            ds.WriteUInt32((uint)ConnectionContext.InContextOptions1);
            ds.WriteUInt32(ConnectionContext.InContextOptions2);
            ds.WriteString(ConnectionContext._clientVproc);

            ds.WriteString(_clientUsername);
            //ds.WriteString(encoder.GetBytes(ccExtention, encoder.Transport));
        }

        public int PrepareMessageParams(EsgynDBEncoder enc)
        {
            int len = 22; //5*4 Int32, 1*2 Int16
            encoder = enc;
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
