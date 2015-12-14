namespace EsgynDB.Data
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    /// <summary>
    /// Collects information relevant to a warning or error returned by EsgynDB.
    /// </summary>
    public sealed class EsgynDBError
    {
        private int _errorCode;
        private string _message;
        private int _rowId;
        private string _state;

        internal EsgynDBError()
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
            return this._message + ", Error code: " + this.ErrorCode + ", State: " + this.State + ", Row index: " + this.RowId;
        }
    }

    /// <summary>
    /// A collection of HPDbErrors.
    /// </summary>
    public sealed class EsgynDBErrorCollection : ICollection, IEnumerable
    {
        private List<EsgynDBError> _errors;
        private object _syncObject;

        internal EsgynDBErrorCollection()
        {
            this._syncObject = new object();
            this._errors = new List<EsgynDBError>();
        }

        /// <summary>
        /// Gets the number of errors in the collection.
        /// </summary>
        public int Count
        {
            get { return this._errors.Count; }
        }

        /// <summary>
        /// Gets a value indicating whether access to the EsgynDBErrorCollection is synchronized (thread safe).
        /// </summary>
        public bool IsSynchronized
        {
            get { return true; }
        }

        /// <summary>
        /// Gets an object that can be used to synchronize access to the EsgynDBErrorCollection.
        /// </summary>
        public object SyncRoot
        {
            get { return this._syncObject; }
        }

        /// <summary>
        /// Gets the EsgynDBError at the given ordinal.
        /// </summary>
        /// <param name="ordinal">The zero-based ordinal.</param>
        /// <returns>The EsgynDBError at the given ordinal.</returns>
        public EsgynDBError this[int ordinal]
        {
            get { return this._errors[ordinal]; }
        }

        /// <summary>
        /// Copies the elements of the EsgynDBErrorCollection collection into an Array, starting at the specified index.
        /// </summary>
        /// <param name="array">The Array to copy elements into. </param>
        /// <param name="index">The index from which to start copying into the array parameter. </param>
        public void CopyTo(Array array, int index)
        {
            this._errors.CopyTo((EsgynDBError[])array, index);
        }

        /// <summary>
        /// Returns an enumerator that iterates through the EsgynDBErrorCollection.
        /// </summary>
        /// <returns>An enumerator that iterates through the EsgynDBErrorCollection.</returns>
        public IEnumerator GetEnumerator()
        {
            return this._errors.GetEnumerator();
        }

        internal void Add(EsgynDBError error)
        {
            this._errors.Add(error);
        }
    }
}