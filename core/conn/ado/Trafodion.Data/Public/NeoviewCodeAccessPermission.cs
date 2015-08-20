/**********************************************************************
// @@@ START COPYRIGHT @@@
//
//        HP CONFIDENTIAL: NEED TO KNOW ONLY
//
//        Copyright 2009-2010
//        Hewlett-Packard Development Company, L.P.
//        Protected as an unpublished work.
//
//  The computer program listings, specifications and documentation
//  herein are the property of Hewlett-Packard Development Company,
//  L.P., or a third party supplier and shall not be reproduced,
//  copied, disclosed, or used in whole or in part for any reason
//  without the prior express written permission of Hewlett-Packard
//  Development Company, L.P.
//
// @@@ END COPYRIGHT @@@
********************************************************************/

using System;
using System.Security;
using System.Security.Permissions;

namespace HP.Data.Neoview
{
    [Serializable]
    public sealed class NeoviewCodeAccessPermission : CodeAccessPermission, IUnrestrictedPermission
    {
        public NeoviewCodeAccessPermission(PermissionState state)
        {
            NeoviewTrace.Trace(null, TraceLevel.Public);
        }
        public override IPermission Copy()
        {
            throw new NotImplementedException();
        }

        public override void FromXml(SecurityElement elem)
        {
            throw new NotImplementedException();
        }

        public override IPermission Intersect(IPermission target)
        {
            throw new NotImplementedException();
        }

        public override bool IsSubsetOf(IPermission target)
        {
            throw new NotImplementedException();
        }

        public override SecurityElement ToXml()
        {
            throw new NotImplementedException();
        }

        public override IPermission Union(IPermission other)
        {
            throw new NotImplementedException();
        }

        public bool IsUnrestricted()
        {
            throw new NotImplementedException();
        }
    }
}
