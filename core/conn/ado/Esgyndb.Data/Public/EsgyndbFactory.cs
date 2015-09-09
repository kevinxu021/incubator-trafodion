namespace Esgyndb.Data
{
    using System;
    using System.Data.Common;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Represents a set of methods for creating instances of the <c>HP.Data</c> provider's implementation of the data source classes.
    /// </summary>
    /// //[Guid("18E26E64-50F9-4ba3-8530-DBBD117B1DCD")]
    [Guid("CD57E4B4-13EC-4FFF-87F9-34F743D8EE7D")]
    public sealed class EsgyndbFactory : DbProviderFactory
    {
        /// <summary>
        /// Gets an instance of the EsgyndbFactory. This can be used to retrieve strongly typed data objects.
        /// </summary>
        public static readonly EsgyndbFactory Instance = new EsgyndbFactory();

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
            return new EsgyndbCommand();
        }

        /// <summary>
        /// Creates a new <c>EsgyndbCommandBuilder</c> object.
        /// </summary>
        /// <returns>A new <c>EsgyndbCommandBuilder</c> object.</returns>
        public override DbCommandBuilder CreateCommandBuilder()
        {
            return new EsgyndbCommandBuilder();
        }

        /// <summary>
        /// Creates a new <c>EsgyndbConnection</c> object.
        /// </summary>
        /// <returns>A new <c>EsgyndbConnection</c> object.</returns>
        public override DbConnection CreateConnection()
        {
            return new EsgyndbConnection();
        }

        /// <summary>
        /// Creates a new <c>EsgyndbConnectionStringBuilder</c> object.
        /// </summary>
        /// <returns>A new <c>EsgyndbConnectionStringBuilder</c> object.</returns>
        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
        {
            return new EsgyndbConnectionStringBuilder();
        }

        /// <summary>
        /// Creates a new <c>EsgyndbDataAdapter</c> object.
        /// </summary>
        /// <returns>A new <c>EsgyndbDataAdapter</c> object.</returns>
        public override DbDataAdapter CreateDataAdapter()
        {
            return new EsgyndbDataAdapter();
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
        /// Creates a new <c>EsgyndbParameter</c> object.
        /// </summary>
        /// <returns>A new <c>EsgyndbParameter</c> object.</returns>
        public override DbParameter CreateParameter()
        {
            return new EsgyndbParameter();
        }
    }
}