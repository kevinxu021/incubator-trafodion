namespace Esgyndb.Data
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;

    /// <summary>
    /// Represents a collection of parameters associated with a EsgyndbCommand and their respective mappings to columns in a DataSet. This class cannot be inherited.
    /// </summary>
    public sealed class EsgyndbParameterCollection : DbParameterCollection, IDataParameterCollection
    {
        private EsgyndbCommand _cmd;
        private List<EsgyndbParameter> _parameters;
        private object _syncObject;

        internal EsgyndbParameterCollection(EsgyndbCommand cmd)
        {
            this._parameters = new List<EsgyndbParameter>();
            this._syncObject = new object();

            this._cmd = cmd;
        }

        /// <summary>
        /// Returns an Integer that contains the number of elements in the EsgyndbParameterCollection. Read-only.
        /// </summary>
        public override int Count
        {
            get { return this._parameters.Count; }
        }

        /// <summary>
        /// Gets a value that indicates whether the EsgyndbParameterCollection has a fixed size.
        /// </summary>
        public override bool IsFixedSize
        {
            get { return false; }
        }

        /// <summary>
        /// Gets a value that indicates whether the EsgyndbParameterCollection is read-only.
        /// </summary>
        public override bool IsReadOnly
        {
            get { return false; }
        }

        /// <summary>
        /// Gets a value that indicates whether the EsgyndbParameterCollection is synchronized.
        /// </summary>
        public override bool IsSynchronized
        {
            get { return false; }
        }

        /// <summary>
        /// Gets an object that can be used to synchronize access to the EsgyndbParameterCollection.
        /// </summary>
        public override object SyncRoot
        {
            get { return this._syncObject; }
        }

        internal List<EsgyndbParameter> Parameters
        {
            get { return this._parameters; }
        }

        /// <summary>
        /// Gets the EsgyndbParameter with the specified name.
        /// </summary>
        /// <param name="parameterName">The name of the parameter to retrieve. </param>
        /// <returns>The EsgyndbParameter with the specified name.</returns>
        /// <exception cref="IndexOutOfRangeException">The specified parameterName is not valid.</exception>
        public new EsgyndbParameter this[string parameterName]
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
        /// Gets the EsgyndbParameter at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index of the parameter to retrieve.</param>
        /// <returns>The EsgyndbParameter at the specified index.</returns>
        /// <exception cref="IndexOutOfRangeException">The specified index does not exist.</exception>
        public new EsgyndbParameter this[int index]
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
        /// Adds the specified EsgyndbParameter object to the EsgyndbParameterCollection.
        /// </summary>
        /// <param name="value">The EsgyndbParameter to add to the collection. </param>
        /// <returns>The index of the new EsgyndbParameter object.</returns>
        /// <exception cref="InvalidCastException">The parameter passed was not a EsgyndbParameter.</exception>
        /// <exception cref="ArgumentNullException">The value parameter is null.</exception>
        public override int Add(object value)
        {
            if (EsgyndbTrace.IsPublicEnabled)
            {
                EsgyndbTrace.Trace(this._cmd.Connection, TraceLevel.Public, value);
            }

            if (value == null)
            {
                EsgyndbException.ThrowException(null, new ArgumentNullException("value"));
            }

            EsgyndbParameter param = (EsgyndbParameter)value;

            this._parameters.Add(param);
            this._cmd.IsPrepared = false;

            return this._parameters.IndexOf(param);
        }

        /// <summary>
        /// Adds an Enumerable Collection of EsgyndbParameter objects to the end of the EsgyndbParameterCollection.
        /// </summary>
        /// <param name="values">The Enumerable Collection of EsgyndbParameter objects to add.</param>
        /// <exception cref="InvalidCastException">The parameter passed was not an Enumerable Collection of EsgyndbParameter objects.</exception>
        /// <exception cref="ArgumentNullException">The values parameter is null.</exception>
        public override void AddRange(Array values)
        {
            if (EsgyndbTrace.IsPublicEnabled)
            {
                EsgyndbTrace.Trace(this._cmd.Connection, TraceLevel.Public, values);
            }

            if (values == null)
            {
                EsgyndbException.ThrowException(null, new ArgumentNullException("values"));
            }

            foreach (EsgyndbParameter p in (IEnumerable<EsgyndbParameter>)values)
            {
                this.Add(p);
            }
        }

        /// <summary>
        /// Removes all the EsgyndbParameter objects from the EsgyndbParameterCollection.
        /// </summary>
        public override void Clear()
        {
            if (EsgyndbTrace.IsPublicEnabled)
            {
                EsgyndbTrace.Trace(this._cmd.Connection, TraceLevel.Public);
            }

            this._parameters.Clear();
            this._cmd.IsPrepared = false;
        }

        /// <summary>
        /// Gets a value indicating whether a EsgyndbParameter in this EsgyndbParameterCollection has the specified name.
        /// </summary>
        /// <param name="value">The name of the EsgyndbParameter. </param>
        /// <returns>true if the HPDbParameterCollections contains the EsgyndbParameter; otherwise false.</returns>
        public override bool Contains(string value)
        {
            return this.IndexOf(value) != -1;
        }

        /// <summary>
        /// Determines whether the specified EsgyndbParameter is in this EsgyndbParameterCollection.
        /// </summary>
        /// <param name="value">The EsgyndbParameter value.</param>
        /// <returns>true if the HPDbParameterCollections contains the EsgyndbParameter; otherwise false.</returns>
        /// <exception cref="InvalidCastException">The parameter passed was not a EsgyndbParameter.</exception>
        public override bool Contains(object value)
        {
            return this._parameters.Contains((EsgyndbParameter)value);
        }

        /// <summary>
        /// Copies all the elements of the current EsgyndbParameterCollection to the specified EsgyndbParameterCollection starting at the specified destination index.
        /// </summary>
        /// <param name="array">The EsgyndbParameterCollection that is the destination of the elements copied from the current EsgyndbParameterCollection.</param>
        /// <param name="index">A 32-bit integer that represents the index in the EsgyndbParameterCollection at which copying starts.</param>
        /// <exception cref="InvalidCastException">The parameter passed was not a EsgyndbParameterCollection.</exception>
        public override void CopyTo(Array array, int index)
        {
            this._parameters.CopyTo((EsgyndbParameter[])array, index);
        }

        /// <summary>
        /// Returns an enumerator that iterates through the EsgyndbParameterCollection.
        /// </summary>
        /// <returns>An IEnumerator for the EsgyndbParameterCollection.</returns>
        public override IEnumerator GetEnumerator()
        {
            return this._parameters.GetEnumerator();
        }

        /// <summary>
        /// Gets the location of the specified EsgyndbParameter with the specified name.
        /// </summary>
        /// <param name="parameterName">The case-sensitive name of the EsgyndbParameter to find.</param>
        /// <returns>The zero-based location of the specified EsgyndbParameter with the specified case-sensitive name. Returns -1 when the object does not exist in the EsgyndbParameterCollection.</returns>
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
        /// Gets the location of the specified EsgyndbParameter within the collection.
        /// </summary>
        /// <param name="value">The EsgyndbParameter to find.</param>
        /// <returns>The zero-based location of the specified EsgyndbParameter that is a EsgyndbParameter within the collection. Returns -1 when the object does not exist in the EsgyndbParameterCollection.</returns>
        /// <exception cref="InvalidCastException">The parameter passed was not a EsgyndbParameter.</exception>
        public override int IndexOf(object value)
        {
            return this.IndexOf(((EsgyndbParameter)value).ParameterName);
        }

        /// <summary>
        /// Inserts a EsgyndbParameter object into the EsgyndbParameterCollection at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index at which value should be inserted.</param>
        /// <param name="value">A EsgyndbParameter object to be inserted in the EsgyndbParameterCollection.</param>
        /// <exception cref="InvalidCastException">The parameter passed was not a EsgyndbParameter.</exception>
        public override void Insert(int index, object value)
        {
            if (EsgyndbTrace.IsPublicEnabled)
            {
                EsgyndbTrace.Trace(this._cmd.Connection, TraceLevel.Public, index, value);
            }

            this._parameters.Insert(index, (EsgyndbParameter)value);
            this._cmd.IsPrepared = false;
        }

        /// <summary>
        /// Removes the specified EsgyndbParameter from the collection.
        /// </summary>
        /// <param name="value">A EsgyndbParameter object to remove from the collection.</param>
        /// <exception cref="InvalidCastException">The parameter passed was not a EsgyndbParameter.</exception>
        public override void Remove(object value)
        {
            if (EsgyndbTrace.IsPublicEnabled)
            {
                EsgyndbTrace.Trace(this._cmd.Connection, TraceLevel.Public, value);
            }

            EsgyndbParameter param = (EsgyndbParameter)value;
            if (this._parameters.Remove(param))
            {
                this._cmd.IsPrepared = false;
            }
        }

        /// <summary>
        /// Removes the EsgyndbParameter from the EsgyndbParameterCollection at the specified parameter name.
        /// </summary>
        /// <param name="parameterName">The name of the EsgyndbParameter to remove.</param>
        /// <exception cref="ArgumentOutOfRangeException">The specified index does not exist.</exception>
        public override void RemoveAt(string parameterName)
        {
            if (EsgyndbTrace.IsPublicEnabled)
            {
                EsgyndbTrace.Trace(this._cmd.Connection, TraceLevel.Public, parameterName);
            }

            this._parameters.RemoveAt(this.IndexOf(parameterName));
            this._cmd.IsPrepared = false;
        }

        /// <summary>
        /// Removes the EsgyndbParameter from the EsgyndbParameterCollection at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index of the EsgyndbParameter object to remove.</param>
        /// <exception cref="ArgumentOutOfRangeException">The specified index does not exist.</exception>
        public override void RemoveAt(int index)
        {
            if (EsgyndbTrace.IsPublicEnabled)
            {
                EsgyndbTrace.Trace(this._cmd.Connection, TraceLevel.Public, index);
            }

            this._parameters.RemoveAt(index);
            this._cmd.IsPrepared = false;
        }

        internal void Prepare(Descriptor[] desc)
        {
            if (EsgyndbTrace.IsInternalEnabled)
            {
                EsgyndbTrace.Trace(this._cmd.Connection, TraceLevel.Internal);
            }

            if (this._parameters.Count != desc.Length && this._cmd.isRWRS == false)
            {
                string msg = EsgyndbResources.FormatMessage(EsgyndbMessage.ParameterCountMismatch);
                EsgyndbException.ThrowException(this._cmd.Connection, new InvalidOperationException(msg));
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
            this[parameterName] = (EsgyndbParameter)value;
        }

        /// <summary>
        /// Sets the DbParameter object at the specified index to a new value.
        /// </summary>
        /// <param name="index">The index where the DbParameter object is located.</param>
        /// <param name="value">The new DbParameter value.</param>
        protected override void SetParameter(int index, DbParameter value)
        {
            this[index] = (EsgyndbParameter)value;
        }
    }
}