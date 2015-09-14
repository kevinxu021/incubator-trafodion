namespace EsgynDB.Data
{
    using System;
    using System.Data;
    using System.Data.Common;

    /// <summary>
    /// Represents a SQL transaction to be made in the database. This class cannot be inherited.
    /// </summary>
    public sealed class EsgynDBTransaction : DbTransaction, IDbTransaction
    {
        private EsgynDBConnection _conn;
        private IsolationLevel _isoLevel;

        internal EsgynDBTransaction(EsgynDBConnection conn, IsolationLevel isolation)
        {
            if (EsgynDBTrace.IsPublicEnabled)
            {
                EsgynDBTrace.Trace(null, TraceLevel.Public, isolation);
            }

            this._conn = conn;
            this._isoLevel = isolation;

            // an unspecified IsolationLevel means we should use the current settings
            if (isolation != IsolationLevel.Unspecified)
            {
                this.SetIsolationLevel(isolation);
            }

            this.SetAutoCommit(false);
        }

        /// <summary>
        /// Gets the EsgynDBConnection object associated with the transaction, or null if the transaction is no longer valid.
        /// </summary>
        public new EsgynDBConnection Connection 
        { 
            get 
            { 
                return this._conn; 
            } 
        }

        /// <summary>
        /// Specifies the IsolationLevel for this transaction.
        /// </summary>
        public override IsolationLevel IsolationLevel 
        { 
            get 
            { 
                return this._isoLevel; 
            }
        }

        /// <summary>
        /// Specifies the DbConnection object associated with the transaction.
        /// </summary>
        protected override DbConnection DbConnection
        {
            get
            {
                return this._conn;
            }
        }

        /// <summary>
        /// Commits the database transaction. 
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// The transaction has already been committed or rolled back. 
        /// -or- 
        /// The connection is broken.
        /// </exception>
        /// <exception cref="EsgynDBException">An error occurred while trying to commit the transaction.</exception>
        public override void Commit()
        {
            if (EsgynDBTrace.IsPublicEnabled)
            {
                EsgynDBTrace.Trace(this._conn, TraceLevel.Public);
            }

            if (this._conn == null)
            {
                string msg = EsgynDBResources.FormatMessage(EsgynDBMessage.InvalidTransactionState);
                EsgynDBException.ThrowException(this._conn, new InvalidOperationException(msg));
            }

            if (this._conn.State != ConnectionState.Open)
            {
                string msg = EsgynDBResources.FormatMessage(EsgynDBMessage.InvalidConnectionState, this._conn.State);
                EsgynDBException.ThrowException(this._conn, new InvalidOperationException(msg));
            }

            this.EndTransaction(EndTransactionOption.Commit);
            this.SetAutoCommit(true);

            this._conn.Transaction = null;
            this._conn = null;
        }

        /// <summary>
        /// Rolls back a transaction from a pending state.
        /// </summary>
        /// /// <exception cref="InvalidOperationException">
        /// The transaction has already been committed or rolled back. 
        /// -or- 
        /// The connection is broken.
        /// </exception>
        /// <exception cref="EsgynDBException">An error occurred while trying to rollback the transaction.</exception>
        public override void Rollback()
        {
            if (EsgynDBTrace.IsPublicEnabled)
            {
                EsgynDBTrace.Trace(this._conn, TraceLevel.Public);
            }

            if (this._conn == null)
            {
                string msg = EsgynDBResources.FormatMessage(EsgynDBMessage.InvalidTransactionState);
                EsgynDBException.ThrowException(this._conn, new InvalidOperationException(msg));
            }

            if (this._conn.State != ConnectionState.Open)
            {
                string msg = EsgynDBResources.FormatMessage(EsgynDBMessage.InvalidConnectionState, this._conn.State);
                EsgynDBException.ThrowException(this._conn, new InvalidOperationException(msg));
            }

            this.EndTransaction(EndTransactionOption.Rollback);
            this.SetAutoCommit(true);

            this._conn.Transaction = null;
            this._conn = null;
        }

        private void EndTransaction(EndTransactionOption opt)
        {
            if (EsgynDBTrace.IsInternalEnabled)
            {
                EsgynDBTrace.Trace(this._conn, TraceLevel.Internal, opt);
            }

            EndTransactionMessage message;
            EndTransactionReply reply;

            message = new EndTransactionMessage()
            {
                Option = opt
            };

            reply = this._conn.Network.EndTransaction(message);

            if (reply.error == EndTransactionError.SqlError)
            {
                throw new EsgynDBException(reply.errorDesc);
            }
            else if (reply.error != EndTransactionError.Success)
            {
                string msg = EsgynDBResources.FormatMessage(
                    EsgynDBMessage.InternalError, 
                    "EndTransaction", 
                    reply.error.ToString() + " " + reply.errorDetail + " " + reply.errorText);

                EsgynDBException.ThrowException(this._conn, new InternalFailureException(msg, null));
            }
        }

        private void SetAutoCommit(bool enabled)
        {
            if (EsgynDBTrace.IsInternalEnabled)
            {
                EsgynDBTrace.Trace(this._conn, TraceLevel.Internal, enabled);
            }

            SetConnectionOptMessage message;
            SetConnectionOptReply reply;

            message = new SetConnectionOptMessage()
            {
                Option = ConnectionOption.AutoCommit,
                IntValue = enabled ? 1 : 0,
                StringValue = string.Empty
            };

            reply = this._conn.Network.SetConnectionOpt(message);

            if (reply.error == SetConnectionOptError.SqlError)
            {
                throw new EsgynDBException(reply.errorDesc);
            }
            else if (reply.error != SetConnectionOptError.Success)
            {
                string msg = EsgynDBResources.FormatMessage(
                    EsgynDBMessage.InternalError,
                    "SetAutoCommit",
                    reply.error.ToString() + " " + reply.errorDetail + " " + reply.errorText);

                EsgynDBException.ThrowException(this._conn, new InternalFailureException(msg, null));
            }
        }

        private void SetIsolationLevel(IsolationLevel il)
        {
            if (EsgynDBTrace.IsInternalEnabled)
            {
                EsgynDBTrace.Trace(this._conn, TraceLevel.Internal, il);
            }

            SetConnectionOptMessage message;
            SetConnectionOptReply reply;

            TransactionIsolation neoIl = TransactionIsolation.ReadCommmited;

            switch (il)
            {
                case IsolationLevel.ReadCommitted:
                    neoIl = TransactionIsolation.ReadCommmited;
                    break;
                case IsolationLevel.ReadUncommitted:
                    neoIl = TransactionIsolation.ReadUncommitted;
                    break;
                case IsolationLevel.Serializable:
                    neoIl = TransactionIsolation.Serializable;
                    break;
                case IsolationLevel.RepeatableRead:
                    neoIl = TransactionIsolation.RepeatableRead;
                    break;
                default:
                    string msg = EsgynDBResources.FormatMessage(EsgynDBMessage.UnsupportedIsolationLevel, il.ToString());
                    EsgynDBException.ThrowException(this._conn, new ArgumentException(msg));
                    break;
            }

            message = new SetConnectionOptMessage()
            {
                Option = ConnectionOption.TransactionIsolation,
                IntValue = (int)neoIl,
                StringValue = string.Empty
            };

            reply = this._conn.Network.SetConnectionOpt(message);

            if (reply.error == SetConnectionOptError.SqlError)
            {
                throw new EsgynDBException(reply.errorDesc);
            }
            else if (reply.error != SetConnectionOptError.Success)
            {
                string msg = EsgynDBResources.FormatMessage(
                   EsgynDBMessage.InternalError,
                   "SetIsolationLevel",
                   reply.error.ToString() + " " + reply.errorDetail + " " + reply.errorText);

                EsgynDBException.ThrowException(this._conn, new InternalFailureException(msg, null));
            }
        }
    }
}