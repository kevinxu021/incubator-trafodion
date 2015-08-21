namespace Trafodion.Data
{
    using System.Collections.Generic;

    internal sealed class ConnectionPool
    {
        private Queue<TrafDbConnection> _idlePool;

        private TrafDbConnectionStringBuilder _builder;

        public ConnectionPool(TrafDbConnectionStringBuilder builder)
        {
            this._builder = builder;

            this._idlePool = new Queue<TrafDbConnection>(this._builder.MaxPoolSize);
        }

        public bool AddPooledConnection(TrafDbConnection conn)
        {
            if (this._idlePool.Count >= this._builder.MaxPoolSize)
            {
                this._idlePool.Dequeue().Close(true);
            }

            this._idlePool.Enqueue(conn);

            return true;
        }

        public TrafDbConnection GetPooledConnection()
        {
            TrafDbConnection conn;

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
