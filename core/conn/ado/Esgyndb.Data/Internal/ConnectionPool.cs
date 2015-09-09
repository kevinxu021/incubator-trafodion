namespace Esgyndb.Data
{
    using System.Collections.Generic;

    internal sealed class ConnectionPool
    {
        private Queue<EsgyndbConnection> _idlePool;

        private EsgyndbConnectionStringBuilder _builder;

        public ConnectionPool(EsgyndbConnectionStringBuilder builder)
        {
            this._builder = builder;

            this._idlePool = new Queue<EsgyndbConnection>(this._builder.MaxPoolSize);
        }

        public bool AddPooledConnection(EsgyndbConnection conn)
        {
            if (this._idlePool.Count >= this._builder.MaxPoolSize)
            {
                this._idlePool.Dequeue().Close(true);
            }

            this._idlePool.Enqueue(conn);

            return true;
        }

        public EsgyndbConnection GetPooledConnection()
        {
            EsgyndbConnection conn;

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
