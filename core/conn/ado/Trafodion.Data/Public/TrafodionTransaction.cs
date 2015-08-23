namespace Trafodion.Data
{
    using System;
    using System.Data;
    using System.Data.Common;

    /// <summary>
    /// Represents a SQL transaction to be made in the database. This class cannot be inherited.
    /// </summary>
    public sealed class TrafDbTransaction : DbTransaction, IDbTransaction
    {
        private TrafDbConnection _conn;
        private IsolationLevel _isoLevel;

        internal TrafDbTransaction(TrafDbConnection conn, IsolationLevel isolation)
        {
            if (TrafDbTrace.IsPublicEnabled)
            {
                TrafDbTrace.Trace(null, TraceLevel.Public, isolation);
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
        /// Gets the TrafDbConnection object associated with the transaction, or null if the transaction is no longer valid.
        /// </summary>
        public new TrafDbConnection Connection 
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
        /// <exception cref="TrafDbException">An error occurred while trying to commit the transaction.</exception>
        public override void Commit()
        {
            if (TrafDbTrace.IsPublicEnabled)
            {
                TrafDbTrace.Trace(this._conn, TraceLevel.Public);
            }

            if (this._conn == null)
            {
                string msg = TrafDbResources.FormatMessage(TrafDbMessage.InvalidTransactionState);
                TrafDbException.ThrowException(this._conn, new InvalidOperationException(msg));
            }

            if (this._conn.State != ConnectionState.Open)
            {
                string msg = TrafDbResources.FormatMessage(TrafDbMessage.InvalidConnectionState, this._conn.State);
                TrafDbException.ThrowException(this._conn, new InvalidOperationException(msg));
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
        /// <exception cref="TrafDbException">An error occurred while trying to rollback the transaction.</exception>
        public override void Rollback()
        {
            if (TrafDbTrace.IsPublicEnabled)
            {
                TrafDbTrace.Trace(this._conn, TraceLevel.Public);
            }

            if (this._conn == null)
            {
                string msg = TrafDbResources.FormatMessage(TrafDbMessage.InvalidTransactionState);
                TrafDbException.ThrowException(this._conn, new InvalidOperationException(msg));
            }

            if (this._conn.State != ConnectionState.Open)
            {
                string msg = TrafDbResources.FormatMessage(TrafDbMessage.InvalidConnectionState, this._conn.State);
                TrafDbException.ThrowException(this._conn, new InvalidOperationException(msg));
            }

            this.EndTransaction(EndTransactionOption.Rollback);
            this.SetAutoCommit(true);

            this._conn.Transaction = null;
            this._conn = null;
        }

        private void EndTransaction(EndTransactionOption opt)
        {
            if (TrafDbTrace.IsInternalEnabled)
            {
                TrafDbTrace.Trace(this._conn, TraceLevel.Internal, opt);
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
                throw new TrafDbException(reply.errorDesc);
            }
            else if (reply.error != EndTransactionError.Success)
            {
                string msg = TrafDbResources.FormatMessage(
                    TrafDbMessage.InternalError, 
                    "EndTransaction", 
                    reply.error.ToString() + " " + reply.errorDetail + " " + reply.errorText);

                TrafDbException.ThrowException(this._conn, new InternalFailureException(msg, null));
            }
        }

        private void SetAutoCommit(bool enabled)
        {
            if (TrafDbTrace.IsInternalEnabled)
            {
                TrafDbTrace.Trace(this._conn, TraceLevel.Internal, enabled);
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
                throw new TrafDbException(reply.errorDesc);
            }
            else if (reply.error != SetConnectionOptError.Success)
            {
                string msg = TrafDbResources.FormatMessage(
                    TrafDbMessage.InternalError,
                    "SetAutoCommit",
                    reply.error.ToString() + " " + reply.errorDetail + " " + reply.errorText);

                TrafDbException.ThrowException(this._conn, new InternalFailureException(msg, null));
            }
        }

        private void SetIsolationLevel(IsolationLevel il)
        {
            if (TrafDbTrace.IsInternalEnabled)
            {
                TrafDbTrace.Trace(this._conn, TraceLevel.Internal, il);
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
                    string msg = TrafDbResources.FormatMessage(TrafDbMessage.UnsupportedIsolationLevel, il.ToString());
                    TrafDbException.ThrowException(this._conn, new ArgumentException(msg));
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
                throw new TrafDbException(reply.errorDesc);
            }
            else if (reply.error != SetConnectionOptError.Success)
            {
                string msg = TrafDbResources.FormatMessage(
                   TrafDbMessage.InternalError,
                   "SetIsolationLevel",
                   reply.error.ToString() + " " + reply.errorDetail + " " + reply.errorText);

                TrafDbException.ThrowException(this._conn, new InternalFailureException(msg, null));
            }
        }
    }
}