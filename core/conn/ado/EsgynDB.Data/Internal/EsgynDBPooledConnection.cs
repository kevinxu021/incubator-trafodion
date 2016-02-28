namespace EsgynDB.Data
{
    using System.Collections.Generic;

    internal sealed class PooledConnection
    {
        private EsgynDBConnection _conn;
        public const int AVAILABLE= 0;
        public const int IN_USE = 1;
        private int _status = AVAILABLE;
        //The connection copied to from connection pool. Take a look connection.open()
        public EsgynDBConnection RefConnection;

        public PooledConnection(EsgynDBConnection conn)
        {
            this._conn = conn;
        }

        public EsgynDBConnection Connection
        {
            get
            {
                return this._conn;
            }

            private set
            {
                this._conn = value;
            }
        }

        public int Status
        {
            get
            {
                return this._status;
            }

            set
            {
                this._status = value;
            }
        }
    }

}
