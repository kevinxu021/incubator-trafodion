namespace Esgyndb.Data
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;

    /// <summary>
    /// Represents a set of data commands and a database connection that are used to fill the DataSet and update a Esgyndb database. This class cannot be inherited.
    /// </summary>
    public sealed class EsgyndbDataAdapter : DbDataAdapter, IDbDataAdapter
    {
        private int _updateBatchSize;
        private List<EsgyndbCommand> _commandBatch;
        private List<Object[]> _batchedParams;

        /// <summary>
        /// Initializes a new instance of the EsgyndbDataAdapter class.
        /// </summary>
        public EsgyndbDataAdapter()
        {
            if (EsgyndbTrace.IsPublicEnabled)
            {
                EsgyndbTrace.Trace(null, TraceLevel.Public);
            }
            this._batchedParams = new List<object[]>();
            this._updateBatchSize = 1;
        }

        /// <summary>
        /// Initializes a new instance of the EsgyndbDataAdapter class with the specified EsgyndbCommand as the SelectCommand property.
        /// </summary>
        /// <param name="selectCommand">The EsgyndbCommand to set as the SelectCommand property.</param>
        public EsgyndbDataAdapter(EsgyndbCommand selectCommand)
        {
            if (EsgyndbTrace.IsPublicEnabled)
            {
                EsgyndbTrace.Trace(null, TraceLevel.Public, selectCommand);
            }

            this._batchedParams = new List<object[]>();
            this._updateBatchSize = 1;
            this.SelectCommand = selectCommand;
        }

        /* 
        public EsgyndbDataAdapter(string selectCommandText, string selectConnectionString)
        {
            
        }

        public EsgyndbDataAdapter(string selectCommandText, EsgyndbConnection selectConnection)
        {

        }
        */

        /// <summary>
        /// Occurs during Update before a command is executed against the data source. The attempt to update is made, so the event fires.
        /// </summary>
        public event EsgyndbRowUpdatingEventHandler RowUpdating;

        /// <summary>
        /// Occurs during Update after a command is executed against the data source. The attempt to update is made, so the event fires.
        /// </summary>
        public event EsgyndbRowUpdatedEventHandler RowUpdated;

        //these provide strong typing
        protected override RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow, IDbCommand command, System.Data.StatementType statementType, DataTableMapping tableMapping)
        {
            return new EsgyndbRowUpdatedEventArgs(dataRow, command, statementType, tableMapping);
        }
        protected override RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow, IDbCommand command, System.Data.StatementType statementType, DataTableMapping tableMapping)
        {
            return new EsgyndbRowUpdatingEventArgs(dataRow, command, statementType, tableMapping);
        }

        /// <summary>
        /// Overridden. Raises the RowUpdating event.
        /// </summary>
        /// <param name="value">A EsgyndbRowUpdatingEventArgs that contains the event data.</param>
        protected override void OnRowUpdating(RowUpdatingEventArgs value)
        {
            if (RowUpdating != null)
            {
                RowUpdating(this, (value as EsgyndbRowUpdatingEventArgs));
            }
            if (value.StatementType == System.Data.StatementType.Insert)
            {
                this._batchedParams.Add(value.Row.ItemArray);
            }
        }

        /// <summary>
		/// Overridden. Raises the RowUpdated event.
		/// </summary>
		/// <param name="value">A HPDbUpdatedEventArgs that contains the event data. </param>
        override protected void OnRowUpdated(RowUpdatedEventArgs value)
        {
            if (RowUpdated != null)
            {
                RowUpdated(this, (value as EsgyndbRowUpdatedEventArgs));
            }
            this._batchedParams.Clear();
        }

        public new EsgyndbCommand SelectCommand { get; set; }
        public new EsgyndbCommand InsertCommand { get; set; }
        public new EsgyndbCommand DeleteCommand { get; set; }
        public new EsgyndbCommand UpdateCommand { get; set; }

        /// <summary>
        /// Gets the parameters set by the user when executing an SQL SELECT statement.
        /// </summary>
        public new EsgyndbParameter[] GetFillParameters()
        {
            EsgyndbParameter[] parameters = new EsgyndbParameter[SelectCommand.Parameters.Count];
            for( int index = 0; index < SelectCommand.Parameters.Count; index++ )
            {
                parameters[index] = SelectCommand.Parameters[index];
            }

            return parameters;
        }

        /// <summary>
        /// Returns a EsgyndbParameter from one of the commands in the current batch.
        /// </summary>
        public new EsgyndbParameter GetBatchedParameter(int commandIdentifier, int parameterIndex)
        {
            return (EsgyndbParameter)_commandBatch[commandIdentifier].Parameters[parameterIndex];
        }

        public override int UpdateBatchSize
        {
            get
            {
                return this._updateBatchSize;
            }
            set
            {
                this._updateBatchSize = value;
            }
        }

        protected override void InitializeBatching()
        {
            if (EsgyndbTrace.IsInternalEnabled)
            {
                EsgyndbTrace.Trace(null, TraceLevel.Internal);
            }

            _commandBatch = new List<EsgyndbCommand>();
        }

        protected int AddToBatch(EsgyndbCommand command)
        {
            if (EsgyndbTrace.IsInternalEnabled)
            {
                EsgyndbTrace.Trace(null, TraceLevel.Internal);
            }

            _commandBatch.Add((EsgyndbCommand)((ICloneable)command).Clone());

            return _commandBatch.Count - 1;
        }

        protected override int AddToBatch(IDbCommand command)
        {
            return this.AddToBatch((EsgyndbCommand)command);
        }

        protected override int ExecuteBatch()
        {
            if (EsgyndbTrace.IsInternalEnabled)
            {
                EsgyndbTrace.Trace(null, TraceLevel.Internal);
            }

            int recordsAffected = 0;
            int index = 0;
            if (this.InsertCommand != null)
            {
                EsgyndbCommand command = this._commandBatch[0];
                for (int i = 0; i < this._batchedParams.Count; i++)
                {
                    command.AddBatch(this._batchedParams[i]);
                }

                recordsAffected = command.ExecuteNonQuery();
            }
            else if(this.SelectCommand != null)
            {
                while (index < _commandBatch.Count)
                {
                    EsgyndbCommand command = _commandBatch[index++];
                    recordsAffected += command.ExecuteNonQuery();
                }
            }

            return recordsAffected;
        }

        protected override void ClearBatch()
        {
            if (EsgyndbTrace.IsInternalEnabled)
            {
                EsgyndbTrace.Trace(null, TraceLevel.Internal);
            }

            _commandBatch.Clear();
        }

        protected override void TerminateBatching()
        {
            if (EsgyndbTrace.IsInternalEnabled)
            {
                EsgyndbTrace.Trace(null, TraceLevel.Internal);
            }

            this.ClearBatch();
            this._commandBatch = null;
        }

        protected override int Update(DataRow[] dataRows, DataTableMapping tableMapping)
        {
            if (this.UpdateCommand != null)
            {
                this.UpdateCommand.Prepare();
            }
            if (this.InsertCommand != null)
            {
                this.InsertCommand.Prepare();
            }
            if (this.DeleteCommand != null)
            {
                this.DeleteCommand.Prepare();
            }

            return base.Update(dataRows, tableMapping);
        }

        public new void Dispose()
        {
            base.Dispose();
        }

        IDbCommand IDbDataAdapter.SelectCommand
        {
            get { return this.SelectCommand; }
            set { this.SelectCommand = (EsgyndbCommand)value; }
        }

        IDbCommand IDbDataAdapter.InsertCommand
        {
            get { return this.InsertCommand; }
            set { this.InsertCommand = (EsgyndbCommand)value; }
        }

        IDbCommand IDbDataAdapter.DeleteCommand
        {
            get { return this.DeleteCommand; }
            set { this.DeleteCommand = (EsgyndbCommand)value; }
        }

        IDbCommand IDbDataAdapter.UpdateCommand
        {
            get { return this.UpdateCommand; }
            set { this.UpdateCommand = (EsgyndbCommand)value; }
        }

        IDataParameter[] IDataAdapter.GetFillParameters()
        {
            return this.GetFillParameters();
        }
    }
}
