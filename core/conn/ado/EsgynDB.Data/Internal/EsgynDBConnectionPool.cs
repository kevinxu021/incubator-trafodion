namespace EsgynDB.Data
{
    using System.Collections.Generic;

    internal sealed class ConnectionPool
    {
        private Queue<EsgynDBConnection> _idlePool;

        private EsgynDBConnectionStringBuilder _builder;

        public ConnectionPool(EsgynDBConnectionStringBuilder builder)
        {
            this._builder = builder;

            this._idlePool = new Queue<EsgynDBConnection>(this._builder.MaxPoolSize);
        }

        public bool AddPooledConnection(EsgynDBConnection conn)
        {
            if (this._idlePool.Count >= this._builder.MaxPoolSize)
            {
                this._idlePool.Dequeue().Close(true);
            }

            this._idlePool.Enqueue(conn);

            return true;
        }

        public EsgynDBConnection GetPooledConnection()
        {
            EsgynDBConnection conn;

            while (this._idlePool.Count > 0)
            {
                conn = this._idlePool.Dequeue();
                try
                {
                    conn.ResetIdleTimeout();
                    return conn;
                }
                catch
                {
                    conn.Close(true); // cleanup
                    // return null
                }
            }

            return null;
        }
    }
}
