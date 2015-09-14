namespace EsgynDB.Data
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;

    /// <summary>
    /// Represents a collection of parameters associated with a EsgyndbCommand and their respective mappings to columns in a DataSet. This class cannot be inherited.
    /// </summary>
    public sealed class EsgynDBParameterCollection : DbParameterCollection, IDataParameterCollection
    {
        private EsgynDBCommand _cmd;
        private List<EsgynDBParameter> _parameters;
        private object _syncObject;

        internal EsgynDBParameterCollection(EsgynDBCommand cmd)
        {
            this._parameters = new List<EsgynDBParameter>();
            this._syncObject = new object();

            this._cmd = cmd;
        }

        /// <summary>
        /// Returns an Integer that contains the number of elements in the EsgynDBParameterCollection. Read-only.
        /// </summary>
        public override int Count
        {
            get { return this._parameters.Count; }
        }

        /// <summary>
        /// Gets a value that indicates whether the EsgynDBParameterCollection has a fixed size.
        /// </summary>
        public override bool IsFixedSize
        {
            get { return false; }
        }

        /// <summary>
        /// Gets a value that indicates whether the EsgynDBParameterCollection is read-only.
        /// </summary>
        public override bool IsReadOnly
        {
            get { return false; }
        }

        /// <summary>
        /// Gets a value that indicates whether the EsgynDBParameterCollection is synchronized.
        /// </summary>
        public override bool IsSynchronized
        {
            get { return false; }
        }

        /// <summary>
        /// Gets an object that can be used to synchronize access to the EsgynDBParameterCollection.
        /// </summary>
        public override object SyncRoot
        {
            get { return this._syncObject; }
        }

        internal List<EsgynDBParameter> Parameters
        {
            get { return this._parameters; }
        }

        /// <summary>
        /// Gets the EsgynDBParameter with the specified name.
        /// </summary>
        /// <param name="parameterName">The name of the parameter to retrieve. </param>
        /// <returns>The EsgynDBParameter with the specified name.</returns>
        /// <exception cref="IndexOutOfRangeException">The specified parameterName is not valid.</exception>
        public new EsgynDBParameter this[string parameterName]
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
        /// Gets the EsgynDBParameter at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index of the parameter to retrieve.</param>
        /// <returns>The EsgynDBParameter at the specified index.</returns>
        /// <exception cref="IndexOutOfRangeException">The specified index does not exist.</exception>
        public new EsgynDBParameter this[int index]
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
        /// Adds the specified EsgynDBParameter object to the EsgynDBParameterCollection.
        /// </summary>
        /// <param name="value">The EsgynDBParameter to add to the collection. </param>
        /// <returns>The index of the new EsgynDBParameter object.</returns>
        /// <exception cref="InvalidCastException">The parameter passed was not a EsgynDBParameter.</exception>
        /// <exception cref="ArgumentNullException">The value parameter is null.</exception>
        public override int Add(object value)
        {
            if (EsgynDBTrace.IsPublicEnabled)
            {
                EsgynDBTrace.Trace(this._cmd.Connection, TraceLevel.Public, value);
            }

            if (value == null)
            {
                EsgynDBException.ThrowException(null, new ArgumentNullException("value"));
            }

            EsgynDBParameter param = (EsgynDBParameter)value;

            this._parameters.Add(param);
            this._cmd.IsPrepared = false;

            return this._parameters.IndexOf(param);
        }

        /// <summary>
        /// Adds an Enumerable Collection of EsgynDBParameter objects to the end of the EsgynDBParameterCollection.
        /// </summary>
        /// <param name="values">The Enumerable Collection of EsgynDBParameter objects to add.</param>
        /// <exception cref="InvalidCastException">The parameter passed was not an Enumerable Collection of EsgynDBParameter objects.</exception>
        /// <exception cref="ArgumentNullException">The values parameter is null.</exception>
        public override void AddRange(Array values)
        {
            if (EsgynDBTrace.IsPublicEnabled)
            {
                EsgynDBTrace.Trace(this._cmd.Connection, TraceLevel.Public, values);
            }

            if (values == null)
            {
                EsgynDBException.ThrowException(null, new ArgumentNullException("values"));
            }

            foreach (EsgynDBParameter p in (IEnumerable<EsgynDBParameter>)values)
            {
                this.Add(p);
            }
        }

        /// <summary>
        /// Removes all the EsgynDBParameter objects from the EsgynDBParameterCollection.
        /// </summary>
        public override void Clear()
        {
            if (EsgynDBTrace.IsPublicEnabled)
            {
                EsgynDBTrace.Trace(this._cmd.Connection, TraceLevel.Public);
            }

            this._parameters.Clear();
            this._cmd.IsPrepared = false;
        }

        /// <summary>
        /// Gets a value indicating whether a EsgynDBParameter in this EsgynDBParameterCollection has the specified name.
        /// </summary>
        /// <param name="value">The name of the EsgynDBParameter. </param>
        /// <returns>true if the EsygndbbParameterCollections contains the EsgynDBParameter; otherwise false.</returns>
        public override bool Contains(string value)
        {
            return this.IndexOf(value) != -1;
        }

        /// <summary>
        /// Determines whether the specified EsgynDBParameter is in this EsgynDBParameterCollection.
        /// </summary>
        /// <param name="value">The EsgynDBParameter value.</param>
        /// <returns>true if the EsygndbbParameterCollections contains the EsgynDBParameter; otherwise false.</returns>
        /// <exception cref="InvalidCastException">The parameter passed was not a EsgynDBParameter.</exception>
        public override bool Contains(object value)
        {
            return this._parameters.Contains((EsgynDBParameter)value);
        }

        /// <summary>
        /// Copies all the elements of the current EsgynDBParameterCollection to the specified EsgynDBParameterCollection starting at the specified destination index.
        /// </summary>
        /// <param name="array">The EsgynDBParameterCollection that is the destination of the elements copied from the current EsgynDBParameterCollection.</param>
        /// <param name="index">A 32-bit integer that represents the index in the EsgynDBParameterCollection at which copying starts.</param>
        /// <exception cref="InvalidCastException">The parameter passed was not a EsgynDBParameterCollection.</exception>
        public override void CopyTo(Array array, int index)
        {
            this._parameters.CopyTo((EsgynDBParameter[])array, index);
        }

        /// <summary>
        /// Returns an enumerator that iterates through the EsgynDBParameterCollection.
        /// </summary>
        /// <returns>An IEnumerator for the EsgynDBParameterCollection.</returns>
        public override IEnumerator GetEnumerator()
        {
            return this._parameters.GetEnumerator();
        }

        /// <summary>
        /// Gets the location of the specified EsgynDBParameter with the specified name.
        /// </summary>
        /// <param name="parameterName">The case-sensitive name of the EsgynDBParameter to find.</param>
        /// <returns>The zero-based location of the specified EsgynDBParameter with the specified case-sensitive name. Returns -1 when the object does not exist in the EsgynDBParameterCollection.</returns>
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
        /// Gets the location of the specified EsgynDBParameter within the collection.
        /// </summary>
        /// <param name="value">The EsgynDBParameter to find.</param>
        /// <returns>The zero-based location of the specified EsgynDBParameter that is a EsgynDBParameter within the collection. Returns -1 when the object does not exist in the EsgynDBParameterCollection.</returns>
        /// <exception cref="InvalidCastException">The parameter passed was not a EsgynDBParameter.</exception>
        public override int IndexOf(object value)
        {
            return this.IndexOf(((EsgynDBParameter)value).ParameterName);
        }

        /// <summary>
        /// Inserts a EsgynDBParameter object into the EsgynDBParameterCollection at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index at which value should be inserted.</param>
        /// <param name="value">A EsgynDBParameter object to be inserted in the EsgynDBParameterCollection.</param>
        /// <exception cref="InvalidCastException">The parameter passed was not a EsgynDBParameter.</exception>
        public override void Insert(int index, object value)
        {
            if (EsgynDBTrace.IsPublicEnabled)
            {
                EsgynDBTrace.Trace(this._cmd.Connection, TraceLevel.Public, index, value);
            }

            this._parameters.Insert(index, (EsgynDBParameter)value);
            this._cmd.IsPrepared = false;
        }

        /// <summary>
        /// Removes the specified EsgynDBParameter from the collection.
        /// </summary>
        /// <param name="value">A EsgynDBParameter object to remove from the collection.</param>
        /// <exception cref="InvalidCastException">The parameter passed was not a EsgynDBParameter.</exception>
        public override void Remove(object value)
        {
            if (EsgynDBTrace.IsPublicEnabled)
            {
                EsgynDBTrace.Trace(this._cmd.Connection, TraceLevel.Public, value);
            }

            EsgynDBParameter param = (EsgynDBParameter)value;
            if (this._parameters.Remove(param))
            {
                this._cmd.IsPrepared = false;
            }
        }

        /// <summary>
        /// Removes the EsgynDBParameter from the EsgynDBParameterCollection at the specified parameter name.
        /// </summary>
        /// <param name="parameterName">The name of the EsgynDBParameter to remove.</param>
        /// <exception cref="ArgumentOutOfRangeException">The specified index does not exist.</exception>
        public override void RemoveAt(string parameterName)
        {
            if (EsgynDBTrace.IsPublicEnabled)
            {
                EsgynDBTrace.Trace(this._cmd.Connection, TraceLevel.Public, parameterName);
            }

            this._parameters.RemoveAt(this.IndexOf(parameterName));
            this._cmd.IsPrepared = false;
        }

        /// <summary>
        /// Removes the EsgynDBParameter from the EsgynDBParameterCollection at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index of the EsgynDBParameter object to remove.</param>
        /// <exception cref="ArgumentOutOfRangeException">The specified index does not exist.</exception>
        public override void RemoveAt(int index)
        {
            if (EsgynDBTrace.IsPublicEnabled)
            {
                EsgynDBTrace.Trace(this._cmd.Connection, TraceLevel.Public, index);
            }

            this._parameters.RemoveAt(index);
            this._cmd.IsPrepared = false;
        }

        internal void Prepare(Descriptor[] desc)
        {
            if (EsgynDBTrace.IsInternalEnabled)
            {
                EsgynDBTrace.Trace(this._cmd.Connection, TraceLevel.Internal);
            }

            if (this._parameters.Count != desc.Length && this._cmd.isRWRS == false)
            {
                string msg = EsgynDBResources.FormatMessage(EsgynDBMessage.ParameterCountMismatch);
                EsgynDBException.ThrowException(this._cmd.Connection, new InvalidOperationException(msg));
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
            this[parameterName] = (EsgynDBParameter)value;
        }

        /// <summary>
        /// Sets the DbParameter object at the specified index to a new value.
        /// </summary>
        /// <param name="index">The index where the DbParameter object is located.</param>
        /// <param name="value">The new DbParameter value.</param>
        protected override void SetParameter(int index, DbParameter value)
        {
            this[index] = (EsgynDBParameter)value;
        }
    }
}