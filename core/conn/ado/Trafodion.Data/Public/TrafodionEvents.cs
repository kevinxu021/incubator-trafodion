namespace Trafodion.Data
{
    using System;
    using System.Data;
    using System.Data.Common;

    /// <summary>
    /// The callback for Info and Warning events for a HPDbConnection.
    /// </summary>
    /// <param name="sender">The HPDbConnection sending the event.</param>
    /// <param name="e">The event arguments.</param>
    public delegate void HPDbInfoMessageEventHandler(object sender, TrafDbInfoMessageEventArgs e);

    /// <summary>
    /// Represents the method that will handle the event of a TrafDbDataAdapter.
    /// </summary>
    /// <param name="sender">The object sending the event.</param>
    /// <param name="e">The event arguments.</param>
    public delegate void HPDbRowUpdatingEventHandler(object sender, HPDbRowUpdatingEventArgs e);

    /// <summary>
    /// Represents the method that will handle the event of a TrafDbDataAdapter.
    /// </summary>
    /// <param name="sender">The object sending the event.</param>
    /// <param name="e">The event arguments.</param>
    public delegate void HPDbRowUpdatedEventHandler(object sender, HPDbRowUpdatedEventArgs e);

    /// <summary>
    /// The event arguments for Info and Warning events for a HPDbConnection.
    /// </summary>
    public sealed class TrafDbInfoMessageEventArgs : EventArgs
    {
        internal TrafDbInfoMessageEventArgs(HPDbErrorCollection errors)
        {
            this.Errors = errors;
            this.ErrorCode = errors[0].ErrorCode;
            this.Message = errors[0].Message;
            this.State = errors[0].State;
            this.RowId = errors[0].RowId;
        }

        /// <summary>
        /// Gets the message text of the error.
        /// </summary>
        public string Message { get; private set; }

        /// <summary>
        /// Gets the error code of the error.
        /// </summary>
        public int ErrorCode { get; private set; }

        /// <summary>
        /// Gets the SqlState of the error.
        /// </summary>
        public string State { get; private set; }

        /// <summary>
        /// Gets the RowId of the error.
        /// </summary>
        public int RowId { get; private set; }

        /// <summary>
        /// Gets a collection of all the errors that occured.
        /// </summary>
        public HPDbErrorCollection Errors { get; private set; }
    }

    /// <summary>
    /// Provides data for the RowUpdating event.
    /// </summary>
    public sealed class HPDbRowUpdatingEventArgs : RowUpdatingEventArgs
    {
        /// <summary>
        /// Initializes a new instance of the HPDbRowUpdatingEventArgs class.
        /// </summary>
        /// <param name="row">The DataRow sent through an Update.</param>
        /// <param name="command">The IDbCommand executed when Update is called.</param>
        /// <param name="statementType">One of the StatementType values that specifies the type of query executed.</param>
        /// <param name="tableMapping">The DataTableMapping sent through an Update.</param>
        public HPDbRowUpdatingEventArgs(DataRow row, IDbCommand command, System.Data.StatementType statementType, DataTableMapping tableMapping)
            : base(row, command, statementType, tableMapping)
        {
        }

        /// <summary>
        /// Gets or sets the HPDbCommand executed when Update is called.
        /// </summary>
        public new TrafDbCommand Command
        {
            get { return (TrafDbCommand)base.Command; }
            set { base.Command = value; }
        }
    }

    /// <summary>
    /// Provides data for the RowUpdated event.
    /// </summary>
    public sealed class HPDbRowUpdatedEventArgs : RowUpdatedEventArgs
    {
        /// <summary>
        /// Initializes a new instance of the HPDbRowUpdatedEventArgs class.
        /// </summary>
        /// <param name="row">The DataRow sent through an Update.</param>
        /// <param name="command">The IDbCommand executed when Update is called.</param>
        /// <param name="statementType">One of the StatementType values that specifies the type of query executed.</param>
        /// <param name="tableMapping">The DataTableMapping sent through an Update.</param>
        public HPDbRowUpdatedEventArgs(DataRow row, IDbCommand command, System.Data.StatementType statementType, DataTableMapping tableMapping)
            : base(row, command, statementType, tableMapping)
        {
        }

        /// <summary>
        /// Gets the HPDbCommand executed when Update is called.
        /// </summary>
        public new TrafDbCommand Command
        {
            get { return (TrafDbCommand)base.Command; }
        }
    }
}