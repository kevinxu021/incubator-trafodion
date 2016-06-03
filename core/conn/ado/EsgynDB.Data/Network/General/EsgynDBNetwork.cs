namespace EsgynDB.Data
{
    using System;
    using System.Data;
    using System.Net.Sockets;
    using System.Text;
    using System.Threading;
    using System.Collections.Generic;

    /// <summary>
    /// Network communcations.
    /// </summary>
    internal class EsgynDBNetwork : IDisposable
    {
        private TcpClient _tcpClient;
        private NetworkStream _networkStream;
        private EsgynDBConnection _connection;
        private EsgynDBEncoder _encoder;

        private Header _writeHeader;
        private Header _readHeader;

        private DataStream _ds;
        private ByteOrder _byteOrder;

        private bool _isClosed;
        private int _dialogueId;

        private bool _compression;
        Timer _timer = null;
        public bool isIdleTimeout = false;

        internal EsgynDBConnection Connection
        {
            get { return this._connection; }
        }

        internal int DialogueId
        {
            get { return this._dialogueId; }
            set { this._dialogueId = value; }
        }

        internal ByteOrder ByteOrder
        {
            get { return this._byteOrder; }
            set { this._byteOrder = value; }
        }

        internal EsgynDBEncoder Encoder
        {
            get { return this._encoder; }
            set { this._encoder = value; }
        }

        internal bool IsClosed
        {
            get { return this._isClosed; }
            set { this._isClosed = value; }
        }

        internal EsgynDBNetwork(EsgynDBConnection conn)
        {
            if (EsgynDBTrace.IsInternalEnabled)
            {
                EsgynDBTrace.Trace(conn, TraceLevel.Internal);
            }

            this.isIdleTimeout = false;
            this._connection = conn;
            this._dialogueId = 0;
            this._isClosed = true;
            if (conn.Network == null)
                this._byteOrder = ByteOrder.BigEndian; // assume NSK
            else
                this._byteOrder = conn.Network.ByteOrder;

            this._readHeader = new Header();
            this._ds = new DataStream(this._byteOrder);
            this._encoder = new EsgynDBEncoder(this._byteOrder);
        }

        internal void OpenIO(string server, int port)
        {
            if (EsgynDBTrace.IsInternalEnabled)
            {
                EsgynDBTrace.Trace(this._connection, TraceLevel.Internal, server, port);
            }

            this._compression = this._connection.ConnectionStringBuilder.Compression;

            this._writeHeader = new Header()
            {
                headerType = HeaderType.WriteRequestFirst,
                compressInd = this._compression ? 'Y' : 'N'
            };

            Monitor.Enter(this._ds);

            try
            {
                this._tcpClient = new TcpClient();
                this._tcpClient.LingerState = new LingerOption(false, 0);
                if (this._connection.ConnectionStringBuilder.TcpKeepAlive)
                {
                    this._tcpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                }
                this._tcpClient.Connect(server, port);
                this._networkStream = this._tcpClient.GetStream();
                this._isClosed = false;
            }
            catch (Exception e)
            {
                this._connection.SetState(ConnectionState.Closed);
                EsgynDBException.ThrowException(this._connection, new CommunicationsFailureException(e));
            }

            finally
            {
                Monitor.Exit(this._ds);
            }
        }

        internal void CloseIO()
        {
            if (EsgynDBTrace.IsInternalEnabled)
            {
                EsgynDBTrace.Trace(this._connection, TraceLevel.Internal);
            }

            Monitor.Enter(this._ds);
            try
            {
                this._isClosed = true;
                this._tcpClient.Close();
            }
            catch (Exception e)
            {
                this._connection.SetState(ConnectionState.Closed);
                EsgynDBException.ThrowException(this._connection, new CommunicationsFailureException(e));
            }
            finally
            {
                Monitor.Exit(this._ds);
            }
        }

        internal GetObjRefReply GetObjRef(string server, int port, GetObjRefMessage message)
        {
            int IOTimeout = 0; // no timeout
            //Initialize byte order
            this._byteOrder = ByteOrder.BigEndian;
            this._ds.ByteOrder = ByteOrder.BigEndian;
            this._encoder.ByteOrder = ByteOrder.BigEndian;

            GetObjRefReply reply = new GetObjRefReply();

            this.OpenIO(server, port);
            this.DoIO(message, reply, OperationId.AS_API_GETOBJREF, IOTimeout);
            this.CloseIO();

            return reply;
        }

        internal InitDialogueReply InitDiag(string server, int port, InitDialogueMessage message)
        {
            int IOTimeout = 0; // no timeout
            InitDialogueReply reply = new InitDialogueReply();

            message.DialogueId = this._dialogueId;

            this.OpenIO(server, port);
            this.DoIO(message, reply, OperationId.SRVR_API_SQLCONNECT, IOTimeout);

            return reply;


        }

        internal SetConnectionOptReply SetConnectionOpt(SetConnectionOptMessage message)
        {
            int IOTimeout = 0; // No timeout
            SetConnectionOptReply reply = new SetConnectionOptReply();

            message.DialogueId = this._dialogueId;

            this.DoIO(message, reply, OperationId.SRVR_API_SQLSETCONNECTATTR, IOTimeout);

            return reply;
        }

        internal PrepareReply Prepare(PrepareMessage message, int IOTimeout)
        {
            PrepareReply reply = new PrepareReply();

            message.DialogueId = this._dialogueId;

            this.DoIO(message, reply, OperationId.SRVR_API_SQLPREPARE, IOTimeout);

            return reply;
        }

        internal ExecuteReply Execute(ExecuteMessage message, OperationId id, int IOTimeout)
        {
            ExecuteReply reply = new ExecuteReply();

            message.DialogueId = this._dialogueId;

            this.DoIO(message, reply, id, IOTimeout);

            return reply;
        }

        internal FetchReply Fetch(FetchMessage message, DataStream ds, BufferType type, int IOTimeout)
        {
            FetchReply reply = new FetchReply();

            message.DialogueId = this._dialogueId;
            this.IdleTimerReset();
            this.DoIO(message, reply, OperationId.SRVR_API_SQLFETCH_ROWSET, ds, type, IOTimeout); // is this correct?
            this.IdleTimerStart();
            return reply;
        }

        internal EndTransactionReply EndTransaction(EndTransactionMessage message)
        {
            lock (this._connection.dataAccessLock)
            {
                int IOTimeout = 0; // no timeout
                EndTransactionReply reply = new EndTransactionReply();

                message.DialogueId = this._dialogueId;

                this.DoIO(message, reply, OperationId.SRVR_API_SQLENDTRAN, IOTimeout);

                return reply;
            }
        }

        /*public MetaDataReply MetaData(MetaDataMessage message)
        {
            MetaDataReply reply = new MetaDataReply();

            message.DialogueID = this.DialogueId;

            DoIO(message, reply, OperationId.SRVR_API_GETCATALOGS);

            return reply;
        }*/

        internal CloseReply Close(CloseMessage message)
        {
            int IOTimeout = 0; // no timeout
            CloseReply reply = new CloseReply();

            message.DialogueId = this._dialogueId;

            this.DoIO(message, reply, OperationId.SRVR_API_SQLFREESTMT, IOTimeout);
            this.IdleTimerReset();

            return reply;
        }

        internal CancelReply Cancel(string server, int port, CancelMessage message)
        {
            int IOTimeout = 0; // no timeout
            CancelReply reply = new CancelReply();

            message.DialogueId = this._dialogueId;

            Monitor.Enter(this._ds);
            try
            {
                this.OpenIO(server, port);
                this.DoIO(message, reply, OperationId.AS_API_STOPSRVR, IOTimeout);
                this.CloseIO();
            }
            finally
            {
                Monitor.Exit(this._ds);
            }

            return reply;
        }

        internal TermDialogueReply TermDiag(TermDialogueMessage message)
        {
            int IOTimeout = 0; // no timeout
            TermDialogueReply reply = new TermDialogueReply();

            message.DialogueId = this._dialogueId;

            this.DoIO(message, reply, OperationId.SRVR_API_SQLDISCONNECT, IOTimeout);
            this.IdleTimerReset();

            return reply;
        }

        internal void DoIO(INetworkMessage message, INetworkReply reply, OperationId id, int IOTimeout)
        {
            this.IdleTimerReset();
            this.DoIO(message, reply, id, null, BufferType.Main, IOTimeout);
            this.IdleTimerStart();
        }

        /* Buffer Management
         * 
         * We will probably rarely use BufferType.Alt -- this should be removed if we start passing in Insert Buffers
         * 
         * SEND
         * Compression = False
         *      Write to BufferType.Main
         * Compression = True
         *      Write to BufferType.Main
         *      Compress to BufferType.Alt
         *      
         * RECV
         * Compression = False
         *      Write to BufferType.Fetch (if one was provided) otherwise BufferType.Main
         * 
         * Compression = True
         *      Write to BufferType.Main
         *      Decompress to BufferType.Fetch (if one was provided) otherwise BufferType.Alt
         * 
         * 
         */
        internal void DoIO(INetworkMessage message, INetworkReply reply, OperationId id, DataStream fetchDs, BufferType fetchType, int IOtimeout)
        {
            if (EsgynDBTrace.IsInternalEnabled)
            {
                EsgynDBTrace.Trace(this._connection, TraceLevel.Internal, id);
            }

            int length;
            bool useFetchBuffer = (fetchDs != null);
            // we are going to use manual locking instead of the lock keyword since we need to try/catch this section anyway
            Monitor.Enter(this._ds);

            if (useFetchBuffer)
            {
                this._ds.SetBuffer(BufferType.Fetch, fetchDs.GetBuffer(fetchType));
            }

            if (this._isClosed)
            {
                string msg = EsgynDBResources.GetMessage(EsgynDBMessage.InvalidConnectionState, this._connection.State);
                EsgynDBException.ThrowException(this._connection, new InvalidOperationException(msg));
            }

            bool forceClose = false;
            try
            {
                length = message.PrepareMessageParams(this._encoder);
                this._ds.CurrentBuffer = BufferType.Main;
                this._ds.Position = 0;
                this._writeHeader.operationId = (short)id;
                this._writeHeader.totalLength = length;

                length += Header.Size;

                // ***** BEGIN COMPRESSION *****
                /*if (this._compression)
                {
                    if (this._writeHeader.totalLength > 10000)
                    {
                        this._writeHeader.compressType = 0x14;
                    }
                    else
                    {
                        this._writeHeader.compressType = 0;
                    }
                }*/
                // ***** END COMPRESSION *****

                this._ds.Resize(length);
                this._writeHeader.WriteToDataStream(this._ds);
                message.WriteToDataStream(this._ds);

                if (EsgynDBTrace.IsNetworkEnabled)
                {
                    EsgynDBTrace.Trace(this._connection, TraceLevel.Network, id, "SEND", this.ToHexString(this._ds.Buffer, length));
                }
                // ***** BEGIN COMPRESSION *****
                /*if (this._writeHeader.compressType == 0x14)
                {
                    length = this._ds.Position;
                    this._ds.Compress(1, length, BufferType.Alt); 
                    this._writeHeader.compressLength = this._ds.Position;     
                }*/
                // ***** END COMPRESSION *****

                this._networkStream.Write(this._ds.Buffer, 0, length);

                this._ds.CurrentBuffer = BufferType.Main;
                this._ds.Position = 0;

                Recv(Header.Size, IOtimeout); 

                this._readHeader.ReadFromDataStream(this._ds, this._encoder);

                if (id == OperationId.AS_API_GETOBJREF)
                {
                    // seaquest
                    if (this._readHeader.version == HeaderVersion.ServerLittleEndian)
                    {
                        this._byteOrder = ByteOrder.LittleEndian;
                        this._ds.ByteOrder = ByteOrder.LittleEndian;
                        this._encoder.ByteOrder = ByteOrder.LittleEndian;
                    }
                }

                // ***** BEGIN COMPRESSION *****
                if (this._readHeader.compressType == 0x14)
                {
                    length = this._readHeader.compressLength;
                    this._ds.CurrentBuffer = BufferType.Main;
                }
                else
                {
                    length = this._readHeader.totalLength;

                    this._ds.CurrentBuffer = (useFetchBuffer) ? BufferType.Fetch : BufferType.Main;
                }
                // ***** END COMPRESSION *****

                this._ds.Resize(length); // check expand buffer to make sure we have room
                this._ds.Position = 0;

                Recv(length,0 /* no timeout */);

                // ***** BEGIN COMPRESSION *****
                if (this._readHeader.compressType == 0x14)
                {
                    this._ds.Decompress(1, length, this._readHeader.totalLength, (useFetchBuffer) ? BufferType.Fetch : BufferType.Alt);
                }
                // ***** END COMPRESSION *****

                if (EsgynDBTrace.IsNetworkEnabled)
                {
                    EsgynDBTrace.Trace(this._connection, TraceLevel.Network, id, "RECV", this.ToHexString(this._ds.Buffer, length));
                }

                this._ds.Position = 0;
                reply.ReadFromDataStream(this._ds, this._encoder);

                if (useFetchBuffer)
                {
                    fetchDs.SetBuffer(fetchType, this._ds.GetBuffer(BufferType.Fetch));
                }
            }
            catch (EsgynDBException)
            {
                throw;
            }
            catch (CommunicationsFailureException)
            {
                forceClose = true;
                throw;
            }
            catch (InternalFailureException)
            {
                forceClose = true;
                throw;
            }
            catch (Exception e)
            {
                forceClose = true;
                EsgynDBException.ThrowException(this._connection, new CommunicationsFailureException(e));
            }
            finally
            {
                Monitor.Exit(this._ds);
                if (forceClose)
                {
                    this._connection.Close(true, OperationId.SRVR_API_SQLFREESTMT == id, OperationId.SRVR_API_SQLDISCONNECT == id);
                }
            }
        }

        public void Dispose()
        {
            try
            {
                this._networkStream.Close();
            }
            catch
            {
            }

            try
            {
                this._tcpClient.Close();
            }
            catch
            {
            }
        }

        private string ToHexString(byte[] b, int end)
        {
            StringBuilder hex = new StringBuilder((b.Length / 16 + 1) * (16 * 2 + 15 + 4 + 8 + 2));
            int count = 1;

            hex.Append("\r\n");

            for (int i = 0; i < end; i++)
            {
                hex.AppendFormat("{0:x2} ", b[i]);
                if ((i == end - 1) && (count != 16))
                {
                    int n = end % 16;
                    for (int k = 0; k < (16 - n) * 3 - 1; k++)
                        hex.Append(" ");
                    hex.Append("    ");
                    for (int j = n - 1; j >= 0; j--)
                    {
                        char tmp;
                        if (i < 16)
                            tmp = (char)b[i];
                        else
                            tmp = (char)b[i - j];
                        if (System.Char.IsControl(tmp) || 0x7f < (ushort)tmp)
                            hex.Append(".");
                        else
                        {
                            hex.Append(tmp.ToString());
                        }
                    }
                    hex.Append("\r\n");
                }
                else if (++count > 16)
                {
                    hex.Append("   ");
                    for (int j = 16 - 1; j >= 0; j--)
                    {
                        char tmp = (char)b[i - j];
                        if (System.Char.IsControl(tmp) || 0x7f < (ushort)tmp)
                            hex.Append(".");
                        else
                        {
                            hex.Append(tmp.ToString());
                        }
                    }
                    hex.Append("\r\n");
                    count = 1;
                }
            }

            return hex.ToString();

        }

        private void Recv(int length, int IOTimeout)
        {
            int totalRead = 0;
            int read;
            if( IOTimeout > 0 )
               this._networkStream.ReadTimeout = IOTimeout * 1000;
            else
               this._networkStream.ReadTimeout = Timeout.Infinite;

            do
            {
                read = this._networkStream.Read(this._ds.Buffer, totalRead, length - totalRead);
                totalRead += read;
            }
            while (read > 0 && totalRead < length);

            if (totalRead < length)
            {
                EsgynDBException.ThrowException(this._connection, new CommunicationsFailureException("Socket unexpectedly closed"));
            }
        }
        /// <summary>
        /// Reset IdleTimer
        /// </summary>
        private void IdleTimerReset()
        {
            if (this._timer == null)
            {
                return;
            }
            if (this.isIdleTimeout)
            {
                EsgynDBException.ThrowException(this._connection, new CommunicationsFailureException("Idle Timeout"));
            }
            this._timer.Dispose();
        }
        /// <summary>
        /// if idletimeout is not null and connection is open, them start timer
        /// </summary>
        private void IdleTimerStart()
        {   
            int idleTimeout = this._connection.ConnectionStringBuilder.IdleTimeout;
            if (idleTimeout > 0 && this._connection.State == ConnectionState.Open)
            {
                TimerCallback timerDelegate = new TimerCallback(IdleTimeoutEvent);
                this._timer = new Timer(timerDelegate, this._connection, idleTimeout * 1000, Timeout.Infinite);
            }
        }
        
        /// <summary>
        /// Idle timeout event
        /// Close connection and Throw out an exception
        /// </summary>
        private void IdleTimeoutEvent(object source)
        {
            EsgynDBConnection connection = (EsgynDBConnection)source;
            Monitor.Enter(connection);
            try
            {
                connection.Close(true);
            }
            finally
            {
                Monitor.Exit(connection);
            }
            this.isIdleTimeout = true;
        }
    }
}
