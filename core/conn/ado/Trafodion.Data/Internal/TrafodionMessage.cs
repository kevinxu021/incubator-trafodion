
using System;

namespace Trafodion.Data
{
    //these names correspond directly to the Messages.resx file which contains the associated text

    //24500-24999 reserved for ADO.NET
    internal enum TrafDbMessage
    {
        //generic
        InternalError,   //used twice currently
        CommunicationFailure,

        //getobjref
        NoServerHandle,
        DSNotAvailable,
        PortNotAvailable,
        TryAgain,

        //others
        UnsupportedCommandType,


        //warnings
        ConnectedToDefaultDS,




        InvalidTransactionState,
        InvalidConnectionState,
        InvalidHPDbDbType,
        UnsupportedIsolationLevel,
        ParameterCountMismatch,
        StmtInvalid
    }
}
