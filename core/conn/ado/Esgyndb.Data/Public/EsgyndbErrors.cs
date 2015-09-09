namespace Esgyndb.Data
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    /// <summary>
    /// Collects information relevant to a warning or error returned by Esgyndb.
    /// </summary>
    public sealed class EsgyndbError
    {
        private int _errorCode;
        private string _message;
        private int _rowId;
        private string _state;

        internal EsgyndbError()
        {
        }

        /// <summary>
        /// Gets a number that identifies the type of error.
        /// </summary>
        public int ErrorCode 
        {
            get
            {
                return this._errorCode;
            }

            internal set
            {
                this._errorCode = value;
            }
        }

        /// <summary>
        /// Gets the text describing the error.
        /// </summary>
        public string Message
        {
            get
            {
                return this._message;
            }

            internal set
            {
                this._message = value;
            }
        }

        /// <summary>
        /// Gets the row id associated with this error.
        /// </summary>
        public int RowId
        {
            get
            {
                return this._rowId;
            }

            internal set
            {
                this._rowId = value;
            }
        }

        /// <summary>
        /// Gets the SQL State associated with this error.
        /// </summary>
        public string State
        {
            get
            {
                return this._state;
            }

            internal set
            {
                this._state = value;
            }
        }

        /// <summary>
        /// Gets the text of the error message. 
        /// </summary>
        /// <returns>The message text.</returns>
        public override string ToString()
        {
            return this._message;
        }
    }

    /// <summary>
    /// A collection of HPDbErrors.
    /// </summary>
    public sealed class EsgyndbErrorCollection : ICollection, IEnumerable
    {
        private List<EsgyndbError> _errors;
        private object _syncObject;

        internal EsgyndbErrorCollection()
        {
            this._syncObject = new object();
            this._errors = new List<EsgyndbError>();
        }

        /// <summary>
        /// Gets the number of errors in the collection.
        /// </summary>
        public int Count
        {
            get { return this._errors.Count; }
        }

        /// <summary>
        /// Gets a value indicating whether access to the EsgyndbErrorCollection is synchronized (thread safe).
        /// </summary>
        public bool IsSynchronized
        {
            get { return true; }
        }

        /// <summary>
        /// Gets an object that can be used to synchronize access to the EsgyndbErrorCollection.
        /// </summary>
        public object SyncRoot
        {
            get { return this._syncObject; }
        }

        /// <summary>
        /// Gets the EsgyndbError at the given ordinal.
        /// </summary>
        /// <param name="ordinal">The zero-based ordinal.</param>
        /// <returns>The EsgyndbError at the given ordinal.</returns>
        public EsgyndbError this[int ordinal]
        {
            get { return this._errors[ordinal]; }
        }

        /// <summary>
        /// Copies the elements of the EsgyndbErrorCollection collection into an Array, starting at the specified index.
        /// </summary>
        /// <param name="array">The Array to copy elements into. </param>
        /// <param name="index">The index from which to start copying into the array parameter. </param>
        public void CopyTo(Array array, int index)
        {
            this._errors.CopyTo((EsgyndbError[])array, index);
        }

        /// <summary>
        /// Returns an enumerator that iterates through the EsgyndbErrorCollection.
        /// </summary>
        /// <returns>An enumerator that iterates through the EsgyndbErrorCollection.</returns>
        public IEnumerator GetEnumerator()
        {
            return this._errors.GetEnumerator();
        }

        internal void Add(EsgyndbError error)
        {
            this._errors.Add(error);
        }
    }
}