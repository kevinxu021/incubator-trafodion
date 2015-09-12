namespace EsgynDB.Data
{
    using System;
    using System.Data;
    using System.Data.Common;

    /// <summary>
    /// Automatically generates single-table commands that are used to reconcile changes made to a DataSet with the associated EsgynDB database. This class cannot be inherited.
    /// </summary>
    public sealed class EsgynDBCommandBuilder : DbCommandBuilder
    {
        /// <summary>
        /// Initializes a new instance of the EsgynDBCommandBuilder class.
        /// </summary>
        public EsgynDBCommandBuilder()
        {
            if (EsgynDBTrace.IsPublicEnabled)
            {
                EsgynDBTrace.Trace(null, TraceLevel.Public);
            }

            this.QuotePrefix = this.QuoteSuffix = "\"";
        }

        /// <summary>
        /// Initializes a new instance of the EsgynDBCommandBuilder class with the associated EsgynDBDataAdapter object.
        /// </summary>
        /// <param name="adapter">The EsgynDBDataAdapter to associate with this EsgynDBCommandBuilder</param>
        public EsgynDBCommandBuilder(EsgynDBDataAdapter adapter)
            : this()
        {
            this.DataAdapter = adapter;
        }

        /// <summary>
        /// Gets or sets a EsgynDBDataAdapter object for which SQL statements are automatically generated.
        /// </summary>
        public new EsgynDBDataAdapter DataAdapter
        {
            get { return (EsgynDBDataAdapter)base.DataAdapter; }
            set { base.DataAdapter = value; }
        }

        #region Methods
        /// <summary>
        /// Gets the automatically generated EsgyndbCommand object required to perform deletions on the database.
        /// </summary>
        /// <returns>The automatically generated EsgyndbCommand object required to perform deletions.</returns>
        public new EsgynDBCommand GetDeleteCommand()
        {
            return (EsgynDBCommand)base.GetDeleteCommand();
        }

        /// <summary>
        /// Gets the automatically generated EsgyndbCommand object required to perform updates on the database.
        /// </summary>
        /// <returns>The automatically generated EsgyndbCommand object required to perform updates.</returns>
        public new EsgynDBCommand GetUpdateCommand()
        {
            return (EsgynDBCommand)base.GetUpdateCommand();
        }

        /// <summary>
        /// Gets the automatically generated EsgyndbCommand object required to perform inserts on the database.
        /// </summary>
        /// <returns>The automatically generated EsgyndbCommand object required to perform inserts.</returns>
        public new EsgynDBCommand GetInsertCommand()
        {
            return (EsgynDBCommand)base.GetInsertCommand(false);
        }

        /// <summary>
        /// Given a quoted identifier, returns the correct unquoted form of that identifier. This includes correctly unescaping any embedded quotes in the identifier.
        /// </summary>
        /// <param name="quotedIdentifier">The identifier that will have its embedded quotes removed.</param>
        /// <returns>The unquoted identifier, with embedded quotes properly unescaped.</returns>
        public override string UnquoteIdentifier(string quotedIdentifier)
        {
            if (quotedIdentifier == null) 
            {
                throw new ArgumentNullException("quotedIdentifier");
            }

            if (!quotedIdentifier.StartsWith(QuotePrefix) ||
                !quotedIdentifier.EndsWith(QuoteSuffix))
            {
                return quotedIdentifier;
            }

            if (quotedIdentifier.StartsWith(QuotePrefix))
            {
                quotedIdentifier = quotedIdentifier.Substring(1);
            }

            if (quotedIdentifier.EndsWith(QuoteSuffix))
            {
                quotedIdentifier = quotedIdentifier.Substring(0, quotedIdentifier.Length - 1);
            }

            quotedIdentifier = quotedIdentifier.Replace(QuotePrefix + QuotePrefix, QuotePrefix);

            return quotedIdentifier;
        }

        /// <summary>
        /// Allows the provider implementation of the DbCommandBuilder class to handle additional parameter properties.
        /// </summary>
        /// <param name="parameter">A DbParameter to which the additional modifications are applied. </param>
        /// <param name="row">The DataRow from the schema table provided by GetSchemaTable. </param>
        /// <param name="statementType">The type of command being generated; INSERT, UPDATE or DELETE.</param>
        /// <param name="whereClause">true if the parameter is part of the update or delete WHERE clause, false if it is part of the insert or update values. </param>
        protected override void ApplyParameterInfo(DbParameter parameter, DataRow row, System.Data.StatementType statementType, bool whereClause)
        {
            ((EsgynDBParameter)parameter).DbType = (DbType)row["ProviderType"];
        }

        /// <summary>
        /// Returns the full parameter name, given the partial parameter name. 
        /// </summary>
        /// <param name="parameterName">The partial name of the parameter.</param>
        /// <returns>The full parameter name corresponding to the partial parameter name requested.</returns>
        protected override string GetParameterName(string parameterName)
        {
            return "?";
        }

        /// <summary>
        /// Returns the name of the specified parameter.
        /// </summary>
        /// <param name="parameterOrdinal">The number to be included as part of the parameter's name.</param>
        /// <returns>The name of the parameter with the specified number appended as part of the parameter name.</returns>
        protected override string GetParameterName(int parameterOrdinal)
        {
            return "?";
        }

        /// <summary>
        /// Returns the placeholder for the parameter in the associated SQL statement.
        /// </summary>
        /// <param name="parameterOrdinal">The number to be included as part of the parameter's name.</param>
        /// <returns>The name of the parameter with the specified number appended.</returns>
        protected override string GetParameterPlaceholder(int parameterOrdinal)
        {
            return "?";
        }

        /// <summary>
        /// Registers the EsgynDBCommandBuilder to handle the RowUpdating event for a DbDataAdapter.
        /// </summary>
        /// <param name="adapter">The DbDataAdapter to be used for the update.</param>
        protected override void SetRowUpdatingHandler(DbDataAdapter adapter)
        {
            EsgynDBDataAdapter dataAdapter = adapter as EsgynDBDataAdapter;
            if (adapter != base.DataAdapter)
            {
                dataAdapter.RowUpdating += new EsgynDBRowUpdatingEventHandler(this.RowUpdating);
            }
            else
            {
                dataAdapter.RowUpdating -= new EsgynDBRowUpdatingEventHandler(this.RowUpdating);
            }
        }

        private void RowUpdating(object sender, EsgynDBRowUpdatingEventArgs args)
        {
            this.RowUpdatingHandler(args);
            if (args.StatementType != System.Data.StatementType.Insert ||
                args.StatementType != System.Data.StatementType.Delete ||
                args.StatementType != System.Data.StatementType.Update)
            {
                return;
            }

            if (args.Command.UpdatedRowSource != UpdateRowSource.None ||
                args.Command.UpdatedRowSource != UpdateRowSource.OutputParameters)
            {
                throw new InvalidOperationException("MixingUpdatedRowSource");
            }
        }
        #endregion
    }
}
