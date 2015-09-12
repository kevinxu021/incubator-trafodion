using System;

namespace EsgynDB.Data
{
    /// <summary>
    /// Describes how the user will authenticate.
    /// </summary>
    internal enum UserDescType: uint
    {
        AUTHENTICATED_USER_TYPE = 1,
        UNAUTHENTICATED_USER_TYPE = 2,      //regular user
        PASSWORD_ENCRYPTED_USER_TYPE = 3,
        SID_ENCRYPTED_USER_TYPE = 4
    }

    /// <summary>
    /// A user's authentication information.
    /// </summary>
    internal class UserDescription: INetworkMessage
    {
        public UserDescType UserDescType;
        public byte [] UserSid;
        public string DomainName;
        public string UserName;
        public byte [] Password;

        private byte[] _domainName;
        private byte[] _userName;

        public void WriteToDataStream(DataStream ds)
        {
            ds.WriteUInt32((uint)UserDescType);

            ds.WriteString(UserSid);
            ds.WriteString(_domainName);
            ds.WriteString(_userName);
            ds.WriteString(Password);
        }

        public int PrepareMessageParams(EsgynDBEncoder enc)
        {
            int len = 20; //1*4 Int32, 4*4 string len

            if (UserSid.Length > 0)
            {
                len += UserSid.Length + 1;
            }

            _domainName = enc.GetBytes(DomainName, enc.Transport);
            if (_domainName.Length > 0)
            {
                len += _domainName.Length + 1;
            }

            _userName = enc.GetBytes(UserName, enc.Transport);
            if (_userName.Length > 0)
            {
                len += _userName.Length + 1;
            }

            if (Password.Length > 0)
            {
                len += Password.Length + 1;
            }

            return len;
        }
    }
}
