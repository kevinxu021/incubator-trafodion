namespace Trafodion.Data
{
    using System;
    using System.Data.Common;

    /// <summary>
    /// The exception that is thrown when TrafDb returns a warning or error. This class cannot be inherited.
    /// </summary>
    [Serializable]
    public sealed class TrafDbException : DbException
    {
        private TrafDbErrorCollection _errors;
        private TrafDbErrorCollection _warnings;

        //overwrite the base classes since we cant use thier constructor for everything
        private string _message;
        private int _errorCode;

        internal static void ThrowException(TrafDbConnection conn, Exception e)
        {
            if (TrafDbTrace.IsErrorEnabled)
            {
                TrafDbTrace.Trace(conn, TraceLevel.Error, e);
            }

            throw e;
        }

        internal TrafDbException(TrafDbMessage msg, params object[] objs)
        {
            this._errors = new TrafDbErrorCollection();
            this._warnings = new TrafDbErrorCollection();

            TrafDbError error = new TrafDbError()
            {
                ErrorCode = 0,
                Message = (objs != null)?TrafDbResources.FormatMessage(msg, objs):TrafDbResources.GetMessage(msg),
                RowId = -1,
                State = "00000"
            };

            if (error.ErrorCode > 0)
            {
                this.Warnings.Add(error);
            }
            else
            {
                this.Errors.Add(error);
            }

            SetMembers();


            if (TrafDbTrace.IsErrorEnabled)
            {
                TrafDbTrace.Trace(null, TraceLevel.Error, this);
            }
        }

        internal TrafDbException(ErrorDesc[] list)
        {
            TrafDbError error;

            this._errors = new TrafDbErrorCollection();
            this._warnings = new TrafDbErrorCollection();

            foreach (ErrorDesc d in list)
            {
                error = new TrafDbError()
                {
                    ErrorCode = d.sqlCode,
                    Message = d.errorText,
                    RowId = d.rowId,
                    State = d.sqlState
                };

                if (error.ErrorCode > 0)
                {
                    this._warnings.Add(error);
                }
                else
                {
                    this._errors.Add(error);
                }
            }

            this.SetMembers();

            if (TrafDbTrace.IsErrorEnabled)
            {
                TrafDbTrace.Trace(null, TraceLevel.Error, this);
            }
        }

        internal TrafDbException(SqlWarningOrError[] list)
        {
            TrafDbError error;

            this._errors = new TrafDbErrorCollection();
            this._warnings = new TrafDbErrorCollection();

            foreach (SqlWarningOrError e in list)
            {
                error = new TrafDbError()
                {
                    ErrorCode = e.sqlCode,
                    Message = e.text,
                    RowId = e.rowId,
                    State = e.sqlState
                };

                if (error.ErrorCode > 0)
                {
                    this._warnings.Add(error);
                }
                else
                {
                    this._errors.Add(error);
                }
            }

            this.SetMembers();

            if (TrafDbTrace.IsErrorEnabled)
            {
                TrafDbTrace.Trace(null, TraceLevel.Error, this);
            }
        }

        /// <summary>
        /// Gets a collection of one or more TrafDbError objects that give detailed information about exceptions generated.
        /// </summary>
        public TrafDbErrorCollection Errors 
        {
            get { return this._errors; }
        }

        /// <summary>
        /// Gets a collection of one or more TrafDbError objects that give detailed information about warnings generated.
        /// </summary>
        public TrafDbErrorCollection Warnings 
        {
            get { return this._warnings; }
        }

        //these should be the same as defined in HPError and InfoMessageEventArgs
        public int RowId { get; private set; }
        public override string Message { get { return _message; } }
        public override int ErrorCode { get { return _errorCode; } }
        public string State { get; private set; }

        /// <summary>
        /// Creates and returns a string representation of the current exception. 
        /// </summary>
        /// <returns>A string representation of the current exception.</returns>
        public override string ToString()
        {
            return this._message;
        }

        private void SetMembers()
        {
            //we might get initialized with only warnings
            if (this.Errors.Count > 0)
            {
                this._message = this._errors[0].Message;
                this._errorCode = this._errors[0].ErrorCode;
                this.State = this._errors[0].State;
                this.RowId = this._errors[0].RowId;
            }
            else if(this.Warnings.Count > 0)
            {
                this._message = this._warnings[0].Message;
                this._errorCode = this._warnings[0].ErrorCode;
                this.State = this._warnings[0].State;
                this.RowId = this._warnings[0].RowId;
            }
        }
    }

    /// <summary>
    /// The exception that is thrown when an internal error occurs.
    /// </summary>
    [Serializable]
    public sealed class InternalFailureException : Exception
    {
        internal InternalFailureException(string msg, Exception innerException)
            : base(msg, innerException)
        {

        }
    }

    /// <summary>
    /// The exception that is thrown when a communcations failure occurs
    /// </summary>
    [Serializable]
    public sealed class CommunicationsFailureException : Exception
    {
        internal CommunicationsFailureException(Exception innerException)
            : base("Communications Failure: " + innerException.Message, innerException)
        {

        }

        internal CommunicationsFailureException(string msg)
            : base("Communications Failure: " + msg)
        {

        }
    }
}