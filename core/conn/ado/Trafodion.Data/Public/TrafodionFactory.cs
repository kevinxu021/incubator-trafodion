namespace Trafodion.Data
{
    using System;
    using System.Data.Common;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Represents a set of methods for creating instances of the <c>HP.Data</c> provider's implementation of the data source classes.
    /// </summary>
    /// //[Guid("18E26E64-50F9-4ba3-8530-DBBD117B1DCD")]
    [Guid("CD57E4B4-13EC-4FFF-87F9-34F743D8EE7D")]
    public sealed class TrafDbFactory : DbProviderFactory
    {
        /// <summary>
        /// Gets an instance of the TrafDbFactory. This can be used to retrieve strongly typed data objects.
        /// </summary>
        public static readonly TrafDbFactory Instance = new TrafDbFactory();

        /// <summary>
        /// Returns <c>false</c>.  <c>CreateDataSourceEnumerator()</c> is not supported
        /// </summary>
        public override bool CanCreateDataSourceEnumerator 
        { 
            get { return false; }
        }

        /// <summary>
        /// Creates a new <c>HPDbCommand</c> object.
        /// </summary>
        /// <returns>A new <c>HPDbCommand</c> object.</returns>
        public override DbCommand CreateCommand()
        {
            return new TrafDbCommand();
        }

        /// <summary>
        /// Creates a new <c>TrafDbCommandBuilder</c> object.
        /// </summary>
        /// <returns>A new <c>TrafDbCommandBuilder</c> object.</returns>
        public override DbCommandBuilder CreateCommandBuilder()
        {
            return new TrafDbCommandBuilder();
        }

        /// <summary>
        /// Creates a new <c>HPDbConnection</c> object.
        /// </summary>
        /// <returns>A new <c>HPDbConnection</c> object.</returns>
        public override DbConnection CreateConnection()
        {
            return new TrafDbConnection();
        }

        /// <summary>
        /// Creates a new <c>TrafDbConnectionStringBuilder</c> object.
        /// </summary>
        /// <returns>A new <c>TrafDbConnectionStringBuilder</c> object.</returns>
        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
        {
            return new TrafDbConnectionStringBuilder();
        }

        /// <summary>
        /// Creates a new <c>TrafDbDataAdapter</c> object.
        /// </summary>
        /// <returns>A new <c>TrafDbDataAdapter</c> object.</returns>
        public override DbDataAdapter CreateDataAdapter()
        {
            return new TrafDbDataAdapter();
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        /// <returns>Not Applicable</returns>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public override DbDataSourceEnumerator CreateDataSourceEnumerator()
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Creates a new <c>TrafDbParameter</c> object.
        /// </summary>
        /// <returns>A new <c>TrafDbParameter</c> object.</returns>
        public override DbParameter CreateParameter()
        {
            return new TrafDbParameter();
        }
    }
}