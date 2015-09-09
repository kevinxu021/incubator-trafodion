
using System;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Reflection;

namespace Esgyndb.Data
{
    [Flags]
    internal enum TraceLevel: ushort
    {
        //flags
        Error = 0x0001,
        Warning = 0x0002,
        Public = 0x0004, 
        Internal = 0x0008, 
        Detail = 0x0010,
        Network = 0x0020,
		Config = 0x0040,

        //convenience
        Info = TraceLevel.Error | TraceLevel.Warning | TraceLevel.Public | TraceLevel.Internal,
        Verbose = TraceLevel.Info | TraceLevel.Detail | TraceLevel.Config | TraceLevel.Network,

        Off = 0x0000,
        All = 0xFFFF
    }

    //basic tracing class which supports a single file stream and console output
    //this class is very slow due to the StackTrace object used
    internal class EsgyndbTrace
    {
        //time ~ level ~ connid ~ remoteprocess ~ class:method ~ msg w/ params
        private static string MessageFormat = "{0,-23}      {1,-10}      {2,-10}      {3,-15}      {4}      {5}.{6}     {7}     {8}\r\n";

        private static TraceLevel _level = TraceLevel.Off;
        private static TextWriter _writer = null;
        private static bool _console = false;
        private static string _log = "esgyndb-adonet.log";

        public static bool IsErrorEnabled;
        public static bool IsWarningEnabled;
        public static bool IsPublicEnabled;
        public static bool IsInternalEnabled;
        public static bool IsDetailEnabled;
        public static bool IsNetworkEnabled;

        static EsgyndbTrace()
        {
            string level;
            string[] split;
            level = getVariable("HPDbTraceLevel");
            if (level != null)
            {
                try
                {
                    split = level.Split(new char[] { '|' });
                    foreach (string s in split)
                    {
                        Level |= (TraceLevel)Enum.Parse(typeof(TraceLevel), s.Trim()); //use the Property to update the flags
                    }
                }
                catch
                {
                    Level = TraceLevel.Off;
                }

                try
                {
                    Boolean.TryParse( getVariable("HPDbTraceConsole"), out _console);
                }
                catch
                { }

                if (_level != TraceLevel.Off)
                {
                    try
                    {
                        string traceFile = getVariable("HPDbTraceFile");
                       
                        if (traceFile != null)
                        {
                            _log = traceFile;
                        }
                        try
                        {
                            _writer = new StreamWriter(_log, true);
                        }
                        catch (Exception e)
                        {
                            System.Console.WriteLine(e.ToString());
                        }
                    }
                    catch
                    { }
                }
            }
            else
            {
                Level = TraceLevel.Off;
            }
        }

        private static String getVariable(String key)
        {
            String msg = "\r\n";
            String value = Environment.GetEnvironmentVariable(key);
 
            if (value==null || "".Equals(value.Trim()))
            {
                value = ConfigurationManager.AppSettings.Get(key);
                if ("HPDbTraceLevel".Equals(key.Trim()))
                {
                    msg += "Program configuration file is " + ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None).FilePath + "\r\n";
                }
                if (value == null || "".Equals(value.Trim()))
                {
                    // Get the machine.config file.
                    Configuration machineConfig =
                      ConfigurationManager.OpenMachineConfiguration();
                    if (machineConfig.AppSettings.Settings[key]!=null)
                        value = machineConfig.AppSettings.Settings[key].Value;
                    msg += key + "(machine.config)= " + value + "\r\n";
                    if ( "HPDbTraceFile".Equals(key.Trim()))
                    {
                        msg += "machine.config file is " + machineConfig.FilePath + "\r\n";
                    }
                }
                else
                {
                    msg += key + "(AppSettings)= " + value + "\r\n";
                }
            }
            else
            {
                msg += key + "(ENV)= " + value + "\r\n";
            }
            Trace(TraceLevel.Config, msg);
            return value;
        }

        public static TraceLevel Level 
        {
            get 
            {
                return EsgyndbTrace._level;
            }
            set
            {
                EsgyndbTrace._level = value;

                EsgyndbTrace.IsErrorEnabled = (EsgyndbTrace._level & TraceLevel.Error) > 0;
                EsgyndbTrace.IsWarningEnabled = (EsgyndbTrace._level & TraceLevel.Warning) > 0;
                EsgyndbTrace.IsPublicEnabled = (EsgyndbTrace._level & TraceLevel.Public) > 0;
                EsgyndbTrace.IsInternalEnabled = (EsgyndbTrace._level & TraceLevel.Internal) > 0;
                EsgyndbTrace.IsDetailEnabled = (EsgyndbTrace._level & TraceLevel.Detail) > 0;
                EsgyndbTrace.IsNetworkEnabled = (EsgyndbTrace._level & TraceLevel.Network) > 0;
            }
        }

        public static bool Console
        {
            get
            {
                return _console;
            }
            set
            {
                _console = value;
            }
        }

        public static string File
        {
            get 
            {
                return _log;
            }
            set 
            {
                _log = value;
                if (_writer != null)
                {
                    _writer.Flush();
                    _writer.Close();
                }
                _writer = new StreamWriter(_log, true);
            }
        }

        public static void Trace(TraceLevel level, params object[] p)
        {
            Trace(null, level, p);
        }

        public static void Trace(EsgyndbConnection conn, TraceLevel level, params object[] p)
        {
            if ((EsgyndbTrace.Level & level) == 0)
            {
                return;
            }

            MethodBase mb;

            string msg;
            int connHashCode;
            string connRemoteProcess;

            if (conn != null)
            {
                connHashCode = conn.GetHashCode();
                connRemoteProcess = conn.RemoteProcess;
            }
            else
            {
                connHashCode = 0;
                connRemoteProcess = "";
            }

            StackTrace st = new StackTrace();
            StackFrame[] stFrames = st.GetFrames();

            mb = new StackTrace().GetFrame(1).GetMethod();
            
            msg = string.Format(EsgyndbTrace.MessageFormat,
                DateTime.Now,
                level,
                connHashCode,
                connRemoteProcess,
                AppDomain.GetCurrentThreadId(),
                mb.DeclaringType.Name,
                mb.Name,
                st.ToString(),
                CreateParamString(p)
                );

            if (EsgyndbTrace._console)
            {
                System.Diagnostics.Trace.TraceInformation(msg);
            }

            if (EsgyndbTrace._writer != null)
            {
                _writer.Write(msg);
                _writer.Flush();
            }
        }

        private static string CreateParamString(params object[] p)
        {
            string msg = "";

            if (p.Length > 0)
            {
                msg += "\r\n  ";

                foreach (object o in p)
                {
                    msg += string.Format("\t({0})   {1} \r\n", (o != null) ? o.GetType().ToString() : "null", o);
                }
            }

            return msg;
        }
    }
}