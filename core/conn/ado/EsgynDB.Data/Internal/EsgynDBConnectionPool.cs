namespace EsgynDB.Data
{
    using System.Collections.Generic;
    using System.Collections;
    using System;

    internal sealed class ConnectionPool
    {
        internal List<PooledConnection> _pool;

        private EsgynDBConnectionStringBuilder _builder;

        private static readonly List<string> poolAccessLock = new List<string>();

        public ConnectionPool(EsgynDBConnectionStringBuilder builder)
        {
            this._builder = builder;
            
            this._pool = new List<PooledConnection>(this._builder.MaxPoolSize);
        }

        public bool ReturnConnection(EsgynDBConnection conn)
        {
            lock (poolAccessLock)
            {
                PooledConnection pooledConn = _pool.Find(x => x.Connection.RemoteAddress != null
                    && x.Connection.RemotePort == conn.RemotePort
                    && x.Connection.RemoteProcess.Equals(conn.RemoteProcess));

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

        public void AddConnection(EsgynDBConnection conn)
        {
            lock (poolAccessLock)
            {
                if (_pool.Count >= this._builder.MaxPoolSize)
                {
                    EsgynDBException e = new EsgynDBException(EsgynDBMessage.PoolExhuasted, this._builder.MaxPoolSize);
                    EsgynDBException.ThrowException(conn, e);
                    //waiting for connecting, done in the future
                }
                if (conn != null)
                {
                    EsgynDBConnection newConn = new EsgynDBConnection(conn.ConnectionString);
                    newConn.CopyProperties(conn);
                    PooledConnection pc = new PooledConnection(newConn);
                    pc.Status = PooledConnection.IN_USE;
                    this._pool.Add(pc);
                }
            }
        }

        public EsgynDBConnection GetConnection(EsgynDBConnection conn)
        {
            lock (poolAccessLock)
            {
                PooledConnection pooledConn = _pool.Find(x => x.Status == PooledConnection.AVAILABLE);

                if (pooledConn != null)
                {
                    pooledConn.Status = PooledConnection.IN_USE;
                    try
                    {
                        pooledConn.Connection.ResetIdleTimeout();
                        return pooledConn.Connection;
                    }
                    catch
                    {
                        pooledConn.Connection.Close(true);
                    }
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
            lock (poolAccessLock)
            {
                PooledConnection pooledConn = _pool.Find(x => x.Connection.RemoteAddress != null
                    && x.Connection.RemotePort == conn.RemotePort
                    && x.Connection.RemoteProcess.Equals(conn.RemoteProcess));
                if (pooledConn != null)
                {
                    pooledConn.Status = PooledConnection.DISABLE;
                    return _pool.Remove(pooledConn);
                }
                return true;
            }
        }
    }

}
