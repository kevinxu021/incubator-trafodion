namespace EsgynDB.Data
{
    using System.Collections.Generic;
    using System.Collections;

    internal sealed class ConnectionPool
    {
        private List<PooledConnection> _pool;

        private EsgynDBConnectionStringBuilder _builder;

        private readonly List<string> poolAccessLock = new List<string>();

        public ConnectionPool(EsgynDBConnectionStringBuilder builder)
        {
            this._builder = builder;
            
            this._pool = new List<PooledConnection>(this._builder.MaxPoolSize);
        }

        public bool ReturnConnection(EsgynDBConnection conn)
        {
            lock (this.poolAccessLock)
            {
                PooledConnection pooledConn = _pool.Find(x => x.Connection == conn || x.RefConnection == conn);
                if (pooledConn != null)
                {
                    //Handle connection timeout in the future
                    pooledConn.Status = PooledConnection.AVAILABLE;
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        public EsgynDBConnection GetConnection(EsgynDBConnection conn)
        {
            lock (this.poolAccessLock)
            {
                PooledConnection pooledConn = _pool.Find(x => x.Status == PooledConnection.AVAILABLE);
                if (pooledConn != null)
                {
                    pooledConn.Status = PooledConnection.IN_USE;
                    try
                    {
                        pooledConn.RefConnection = conn;
                        pooledConn.Connection.ResetIdleTimeout();
                        return pooledConn.Connection;
                    }
                    catch
                    {
                        pooledConn.Connection.Close(true);
                        _pool.Remove(pooledConn);
                    }
                }
                if (conn != null && _pool.Count < this._builder.MaxPoolSize)
                {
                    PooledConnection pc = new PooledConnection(conn);
                    pc.Status = PooledConnection.IN_USE;
                    this._pool.Add(pc);
                }
                else if (_pool.Count >= this._builder.MaxPoolSize)
                {
                    EsgynDBException e = new EsgynDBException(EsgynDBMessage.PoolExhuasted, this._builder.MaxPoolSize);
                    EsgynDBException.ThrowException(conn, e);
                    //waiting for connecting, done in the future
                }
                return null;
            }
        }

        internal bool RemoveConnection(EsgynDBConnection conn)
        {
            lock (this.poolAccessLock)
            {
                PooledConnection pooledConn = _pool.Find(x => x.Connection == conn || x.RefConnection == conn);
                if (pooledConn != null)
                    return _pool.Remove(pooledConn);
                return true;
            }
        }
    }

}
