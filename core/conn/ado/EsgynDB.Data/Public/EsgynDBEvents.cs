namespace EsgynDB.Data
{
    using System;
    using System.Data;
    using System.Data.Common;

    /// <summary>
    /// The callback for Info and Warning events for a EsgynDBConnection.
    /// </summary>
    /// <param name="sender">The EsgynDBConnection sending the event.</param>
    /// <param name="e">The event arguments.</param>
    public delegate void EsgynDBInfoMessageEventHandler(object sender, EsgynDBInfoMessageEventArgs e);

    /// <summary>
    /// Represents the method that will handle the event of a EsgynDBDataAdapter.
    /// </summary>
    /// <param name="sender">The object sending the event.</param>
    /// <param name="e">The event arguments.</param>
    public delegate void EsgynDBRowUpdatingEventHandler(object sender, EsgynDBRowUpdatingEventArgs e);

    /// <summary>
    /// Represents the method that will handle the event of a EsgynDBDataAdapter.
    /// </summary>
    /// <param name="sender">The object sending the event.</param>
    /// <param name="e">The event arguments.</param>
    public delegate void EsgynDBRowUpdatedEventHandler(object sender, EsgynDBRowUpdatedEventArgs e);

    /// <summary>
    /// The event arguments for Info and Warning events for a EsgynDBConnection.
    /// </summary>
    public sealed class EsgynDBInfoMessageEventArgs : EventArgs
    {
        internal EsgynDBInfoMessageEventArgs(EsgynDBErrorCollection errors)
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
        public EsgynDBErrorCollection Errors { get; private set; }
    }

    /// <summary>
    /// Provides data for the RowUpdating event.
    /// </summary>
    public sealed class EsgynDBRowUpdatingEventArgs : RowUpdatingEventArgs
    {
        /// <summary>
        /// Initializes a new instance of the EsgynDBRowUpdatingEventArgs class.
        /// </summary>
        /// <param name="row">The DataRow sent through an Update.</param>
        /// <param name="command">The IDbCommand executed when Update is called.</param>
        /// <param name="statementType">One of the StatementType values that specifies the type of query executed.</param>
        /// <param name="tableMapping">The DataTableMapping sent through an Update.</param>
        public EsgynDBRowUpdatingEventArgs(DataRow row, IDbCommand command, System.Data.StatementType statementType, DataTableMapping tableMapping)
            : base(row, command, statementType, tableMapping)
        {
        }

        /// <summary>
        /// Gets or sets the EsgyndbCommand executed when Update is called.
        /// </summary>
        public new EsgynDBCommand Command
        {
            get { return (EsgynDBCommand)base.Command; }
            set { base.Command = value; }
        }
    }

    /// <summary>
    /// Provides data for the RowUpdated event.
    /// </summary>
    public sealed class EsgynDBRowUpdatedEventArgs : RowUpdatedEventArgs
    {
        /// <summary>
        /// Initializes a new instance of the EsgynDBRowUpdatedEventArgs class.
        /// </summary>
        /// <param name="row">The DataRow sent through an Update.</param>
        /// <param name="command">The IDbCommand executed when Update is called.</param>
        /// <param name="statementType">One of the StatementType values that specifies the type of query executed.</param>
        /// <param name="tableMapping">The DataTableMapping sent through an Update.</param>
        public EsgynDBRowUpdatedEventArgs(DataRow row, IDbCommand command, System.Data.StatementType statementType, DataTableMapping tableMapping)
            : base(row, command, statementType, tableMapping)
        {
        }

        /// <summary>
        /// Gets the EsgyndbCommand executed when Update is called.
        /// </summary>
        public new EsgynDBCommand Command
        {
            get { return (EsgynDBCommand)base.Command; }
        }
    }
}