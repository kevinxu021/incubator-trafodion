/**********************************************************************
// @@@ START COPYRIGHT @@@
//
//        HP CONFIDENTIAL: NEED TO KNOW ONLY
//
//        Copyright 2009-2010
//        Hewlett-Packard Development Company, L.P.
//        Protected as an unpublished work.
//
//  The computer program listings, specifications and documentation
//  herein are the property of Hewlett-Packard Development Company,
//  L.P., or a third party supplier and shall not be reproduced,
//  copied, disclosed, or used in whole or in part for any reason
//  without the prior express written permission of Hewlett-Packard
//  Development Company, L.P.
//
// @@@ END COPYRIGHT @@@
********************************************************************/

namespace HP.Data
{
    using System.Collections.Generic;

    internal sealed class ConnectionPool
    {
        private Queue<HPDbConnection> _idlePool;

        private HPDbConnectionStringBuilder _builder;

        public ConnectionPool(HPDbConnectionStringBuilder builder)
        {
            this._builder = builder;

            this._idlePool = new Queue<HPDbConnection>(this._builder.MaxPoolSize);
        }

        public bool AddPooledConnection(HPDbConnection conn)
        {
            if (this._idlePool.Count >= this._builder.MaxPoolSize)
            {
                this._idlePool.Dequeue().Close(true);
            }

            this._idlePool.Enqueue(conn);

            return true;
        }

        public HPDbConnection GetPooledConnection()
        {
            HPDbConnection conn;

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
