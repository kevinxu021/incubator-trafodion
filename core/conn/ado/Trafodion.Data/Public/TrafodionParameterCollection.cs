namespace Trafodion.Data
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;

    /// <summary>
    /// Represents a collection of parameters associated with a HPDbCommand and their respective mappings to columns in a DataSet. This class cannot be inherited.
    /// </summary>
    public sealed class TrafDbParameterCollection : DbParameterCollection, IDataParameterCollection
    {
        private TrafDbCommand _cmd;
        private List<TrafDbParameter> _parameters;
        private object _syncObject;

        internal TrafDbParameterCollection(TrafDbCommand cmd)
        {
            this._parameters = new List<TrafDbParameter>();
            this._syncObject = new object();

            this._cmd = cmd;
        }

        /// <summary>
        /// Returns an Integer that contains the number of elements in the TrafDbParameterCollection. Read-only.
        /// </summary>
        public override int Count
        {
            get { return this._parameters.Count; }
        }

        /// <summary>
        /// Gets a value that indicates whether the TrafDbParameterCollection has a fixed size.
        /// </summary>
        public override bool IsFixedSize
        {
            get { return false; }
        }

        /// <summary>
        /// Gets a value that indicates whether the TrafDbParameterCollection is read-only.
        /// </summary>
        public override bool IsReadOnly
        {
            get { return false; }
        }

        /// <summary>
        /// Gets a value that indicates whether the TrafDbParameterCollection is synchronized.
        /// </summary>
        public override bool IsSynchronized
        {
            get { return false; }
        }

        /// <summary>
        /// Gets an object that can be used to synchronize access to the TrafDbParameterCollection.
        /// </summary>
        public override object SyncRoot
        {
            get { return this._syncObject; }
        }

        internal List<TrafDbParameter> Parameters
        {
            get { return this._parameters; }
        }

        /// <summary>
        /// Gets the TrafDbParameter with the specified name.
        /// </summary>
        /// <param name="parameterName">The name of the parameter to retrieve. </param>
        /// <returns>The TrafDbParameter with the specified name.</returns>
        /// <exception cref="IndexOutOfRangeException">The specified parameterName is not valid.</exception>
        public new TrafDbParameter this[string parameterName]
        {
            get
            {
                return this._parameters[this.IndexOf(parameterName)];
            }

            set
            {
                this._parameters[this.IndexOf(parameterName)] = value;
                this._cmd.IsPrepared = false;
            }
        }

        /// <summary>
        /// Gets the TrafDbParameter at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index of the parameter to retrieve.</param>
        /// <returns>The TrafDbParameter at the specified index.</returns>
        /// <exception cref="IndexOutOfRangeException">The specified index does not exist.</exception>
        public new TrafDbParameter this[int index]
        {
            get
            {
                return this._parameters[index];
            }

            set
            {
                this._parameters[index] = value;
                this._cmd.IsPrepared = false;
            }
        }

        /// <summary>
        /// Adds the specified TrafDbParameter object to the TrafDbParameterCollection.
        /// </summary>
        /// <param name="value">The TrafDbParameter to add to the collection. </param>
        /// <returns>The index of the new TrafDbParameter object.</returns>
        /// <exception cref="InvalidCastException">The parameter passed was not a TrafDbParameter.</exception>
        /// <exception cref="ArgumentNullException">The value parameter is null.</exception>
        public override int Add(object value)
        {
            if (TrafDbTrace.IsPublicEnabled)
            {
                TrafDbTrace.Trace(this._cmd.Connection, TraceLevel.Public, value);
            }

            if (value == null)
            {
                TrafDbException.ThrowException(null, new ArgumentNullException("value"));
            }

            TrafDbParameter param = (TrafDbParameter)value;

            this._parameters.Add(param);
            this._cmd.IsPrepared = false;

            return this._parameters.IndexOf(param);
        }

        /// <summary>
        /// Adds an Enumerable Collection of TrafDbParameter objects to the end of the TrafDbParameterCollection.
        /// </summary>
        /// <param name="values">The Enumerable Collection of TrafDbParameter objects to add.</param>
        /// <exception cref="InvalidCastException">The parameter passed was not an Enumerable Collection of TrafDbParameter objects.</exception>
        /// <exception cref="ArgumentNullException">The values parameter is null.</exception>
        public override void AddRange(Array values)
        {
            if (TrafDbTrace.IsPublicEnabled)
            {
                TrafDbTrace.Trace(this._cmd.Connection, TraceLevel.Public, values);
            }

            if (values == null)
            {
                TrafDbException.ThrowException(null, new ArgumentNullException("values"));
            }

            foreach (TrafDbParameter p in (IEnumerable<TrafDbParameter>)values)
            {
                this.Add(p);
            }
        }

        /// <summary>
        /// Removes all the TrafDbParameter objects from the TrafDbParameterCollection.
        /// </summary>
        public override void Clear()
        {
            if (TrafDbTrace.IsPublicEnabled)
            {
                TrafDbTrace.Trace(this._cmd.Connection, TraceLevel.Public);
            }

            this._parameters.Clear();
            this._cmd.IsPrepared = false;
        }

        /// <summary>
        /// Gets a value indicating whether a TrafDbParameter in this TrafDbParameterCollection has the specified name.
        /// </summary>
        /// <param name="value">The name of the TrafDbParameter. </param>
        /// <returns>true if the HPDbParameterCollections contains the TrafDbParameter; otherwise false.</returns>
        public override bool Contains(string value)
        {
            return this.IndexOf(value) != -1;
        }

        /// <summary>
        /// Determines whether the specified TrafDbParameter is in this TrafDbParameterCollection.
        /// </summary>
        /// <param name="value">The TrafDbParameter value.</param>
        /// <returns>true if the HPDbParameterCollections contains the TrafDbParameter; otherwise false.</returns>
        /// <exception cref="InvalidCastException">The parameter passed was not a TrafDbParameter.</exception>
        public override bool Contains(object value)
        {
            return this._parameters.Contains((TrafDbParameter)value);
        }

        /// <summary>
        /// Copies all the elements of the current TrafDbParameterCollection to the specified TrafDbParameterCollection starting at the specified destination index.
        /// </summary>
        /// <param name="array">The TrafDbParameterCollection that is the destination of the elements copied from the current TrafDbParameterCollection.</param>
        /// <param name="index">A 32-bit integer that represents the index in the TrafDbParameterCollection at which copying starts.</param>
        /// <exception cref="InvalidCastException">The parameter passed was not a TrafDbParameterCollection.</exception>
        public override void CopyTo(Array array, int index)
        {
            this._parameters.CopyTo((TrafDbParameter[])array, index);
        }

        /// <summary>
        /// Returns an enumerator that iterates through the TrafDbParameterCollection.
        /// </summary>
        /// <returns>An IEnumerator for the TrafDbParameterCollection.</returns>
        public override IEnumerator GetEnumerator()
        {
            return this._parameters.GetEnumerator();
        }

        /// <summary>
        /// Gets the location of the specified TrafDbParameter with the specified name.
        /// </summary>
        /// <param name="parameterName">The case-sensitive name of the TrafDbParameter to find.</param>
        /// <returns>The zero-based location of the specified TrafDbParameter with the specified case-sensitive name. Returns -1 when the object does not exist in the TrafDbParameterCollection.</returns>
        public override int IndexOf(string parameterName)
        {
            for (int i = 0; i < this._parameters.Count; i++)
            {
                if (this._parameters[i].ParameterName.Equals(parameterName))
                {
                    return i;
                }
            }

            return -1;
        }

        /// <summary>
        /// Gets the location of the specified TrafDbParameter within the collection.
        /// </summary>
        /// <param name="value">The TrafDbParameter to find.</param>
        /// <returns>The zero-based location of the specified TrafDbParameter that is a TrafDbParameter within the collection. Returns -1 when the object does not exist in the TrafDbParameterCollection.</returns>
        /// <exception cref="InvalidCastException">The parameter passed was not a TrafDbParameter.</exception>
        public override int IndexOf(object value)
        {
            return this.IndexOf(((TrafDbParameter)value).ParameterName);
        }

        /// <summary>
        /// Inserts a TrafDbParameter object into the TrafDbParameterCollection at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index at which value should be inserted.</param>
        /// <param name="value">A TrafDbParameter object to be inserted in the TrafDbParameterCollection.</param>
        /// <exception cref="InvalidCastException">The parameter passed was not a TrafDbParameter.</exception>
        public override void Insert(int index, object value)
        {
            if (TrafDbTrace.IsPublicEnabled)
            {
                TrafDbTrace.Trace(this._cmd.Connection, TraceLevel.Public, index, value);
            }

            this._parameters.Insert(index, (TrafDbParameter)value);
            this._cmd.IsPrepared = false;
        }

        /// <summary>
        /// Removes the specified TrafDbParameter from the collection.
        /// </summary>
        /// <param name="value">A TrafDbParameter object to remove from the collection.</param>
        /// <exception cref="InvalidCastException">The parameter passed was not a TrafDbParameter.</exception>
        public override void Remove(object value)
        {
            if (TrafDbTrace.IsPublicEnabled)
            {
                TrafDbTrace.Trace(this._cmd.Connection, TraceLevel.Public, value);
            }

            TrafDbParameter param = (TrafDbParameter)value;
            if (this._parameters.Remove(param))
            {
                this._cmd.IsPrepared = false;
            }
        }

        /// <summary>
        /// Removes the TrafDbParameter from the TrafDbParameterCollection at the specified parameter name.
        /// </summary>
        /// <param name="parameterName">The name of the TrafDbParameter to remove.</param>
        /// <exception cref="ArgumentOutOfRangeException">The specified index does not exist.</exception>
        public override void RemoveAt(string parameterName)
        {
            if (TrafDbTrace.IsPublicEnabled)
            {
                TrafDbTrace.Trace(this._cmd.Connection, TraceLevel.Public, parameterName);
            }

            this._parameters.RemoveAt(this.IndexOf(parameterName));
            this._cmd.IsPrepared = false;
        }

        /// <summary>
        /// Removes the TrafDbParameter from the TrafDbParameterCollection at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index of the TrafDbParameter object to remove.</param>
        /// <exception cref="ArgumentOutOfRangeException">The specified index does not exist.</exception>
        public override void RemoveAt(int index)
        {
            if (TrafDbTrace.IsPublicEnabled)
            {
                TrafDbTrace.Trace(this._cmd.Connection, TraceLevel.Public, index);
            }

            this._parameters.RemoveAt(index);
            this._cmd.IsPrepared = false;
        }

        internal void Prepare(Descriptor[] desc)
        {
            if (TrafDbTrace.IsInternalEnabled)
            {
                TrafDbTrace.Trace(this._cmd.Connection, TraceLevel.Internal);
            }

            if (this._parameters.Count != desc.Length && this._cmd.isRWRS == false)
            {
                string msg = TrafDbResources.FormatMessage(TrafDbMessage.ParameterCountMismatch);
                TrafDbException.ThrowException(this._cmd.Connection, new InvalidOperationException(msg));
            }
            int idx = 0;
            for (int i = 0; i < desc.Length; i++)
            {
                if (this._cmd.isRWRS && i < 3)
                {
                    idx = 3;
                    continue;
                }
                this._parameters[i - idx].Descriptor = desc[i];
            }
        }

        /// <summary>
        /// Returns DbParameter the object with the specified name.
        /// </summary>
        /// <param name="parameterName">The name of the DbParameter in the collection.</param>
        /// <returns>The DbParameter the object with the specified name.</returns>
        protected override DbParameter GetParameter(string parameterName)
        {
            return this[parameterName];
        }

        /// <summary>
        /// Returns the DbParameter object at the specified index in the collection.
        /// </summary>
        /// <param name="index">The index of the DbParameter in the collection.</param>
        /// <returns>The DbParameter object at the specified index in the collection.</returns>
        protected override DbParameter GetParameter(int index)
        {
            return this[index];
        }

        /// <summary>
        /// Sets the DbParameter object with the specified name to a new value.
        /// </summary>
        /// <param name="parameterName">The name of the DbParameter object in the collection.</param>
        /// <param name="value">The new DbParameter value.</param>
        protected override void SetParameter(string parameterName, DbParameter value)
        {
            this[parameterName] = (TrafDbParameter)value;
        }

        /// <summary>
        /// Sets the DbParameter object at the specified index to a new value.
        /// </summary>
        /// <param name="index">The index where the DbParameter object is located.</param>
        /// <param name="value">The new DbParameter value.</param>
        protected override void SetParameter(int index, DbParameter value)
        {
            this[index] = (TrafDbParameter)value;
        }
    }
}