namespace EsgynDB.Data
{
    using System;
    using System.Data.Common;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Represents a set of methods for creating instances of the <c>HP.Data</c> provider's implementation of the data source classes.
    /// </summary>
    /// //[Guid("18E26E64-50F9-4ba3-8530-DBBD117B1DCD")]
    [Guid("CD57E4B4-13EC-4FFF-87F9-34F743D8EE7D")]
    public sealed class EsgynDBFactory : DbProviderFactory
    {
        /// <summary>
        /// Gets an instance of the EsgynDBFactory. This can be used to retrieve strongly typed data objects.
        /// </summary>
        public static readonly EsgynDBFactory Instance = new EsgynDBFactory();

        /// <summary>
        /// Returns <c>false</c>.  <c>CreateDataSourceEnumerator()</c> is not supported
        /// </summary>
        public override bool CanCreateDataSourceEnumerator 
        { 
            get { return false; }
        }

        /// <summary>
        /// Creates a new <c>EsgyndbCommand</c> object.
        /// </summary>
        /// <returns>A new <c>EsgyndbCommand</c> object.</returns>
        public override DbCommand CreateCommand()
        {
            return new EsgynDBCommand();
        }

        /// <summary>
        /// Creates a new <c>EsgynDBCommandBuilder</c> object.
        /// </summary>
        /// <returns>A new <c>EsgynDBCommandBuilder</c> object.</returns>
        public override DbCommandBuilder CreateCommandBuilder()
        {
            return new EsgynDBCommandBuilder();
        }

        /// <summary>
        /// Creates a new <c>EsgynDBConnection</c> object.
        /// </summary>
        /// <returns>A new <c>EsgynDBConnection</c> object.</returns>
        public override DbConnection CreateConnection()
        {
            return new EsgynDBConnection();
        }

        /// <summary>
        /// Creates a new <c>EsgynDBConnectionStringBuilder</c> object.
        /// </summary>
        /// <returns>A new <c>EsgynDBConnectionStringBuilder</c> object.</returns>
        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
        {
            return new EsgynDBConnectionStringBuilder();
        }

        /// <summary>
        /// Creates a new <c>EsgynDBDataAdapter</c> object.
        /// </summary>
        /// <returns>A new <c>EsgynDBDataAdapter</c> object.</returns>
        public override DbDataAdapter CreateDataAdapter()
        {
            return new EsgynDBDataAdapter();
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
        /// Creates a new <c>EsgynDBParameter</c> object.
        /// </summary>
        /// <returns>A new <c>EsgynDBParameter</c> object.</returns>
        public override DbParameter CreateParameter()
        {
            return new EsgynDBParameter();
        }
    }
}