﻿using Microsoft.VisualStudio.Data;
using Microsoft.VisualStudio.Data.AdoDotNet;

namespace Trafodion.Data.VisualStudio
{
    public class TrafDbDataSourceInformation : AdoDotNetDataSourceInformation
    {
        public TrafDbDataSourceInformation(DataConnection connection)
            : base(connection)
        {
            AddProperty(SupportsAnsi92Sql, true);
            //AddProperty(IdentifierPartsCaseSensitive, false);
            AddProperty(SupportsQuotedIdentifierParts, true);
            AddProperty(IdentifierOpenQuote, "\"");
            AddProperty(IdentifierCloseQuote, "\"");
            AddProperty(QuotedIdentifierPartsStorageCase, "M");
            AddProperty(QuotedIdentifierPartsCaseSensitive, true);
            
            AddProperty(ServerSeparator, ".");
            AddProperty(CatalogSupported, true);
            AddProperty(CatalogSupportedInDml, true);
            AddProperty(CatalogSeparator, ".");
            AddProperty(SchemaSupported, true);
            AddProperty(SchemaSupportedInDml, true);
            AddProperty(SchemaSeparator, ".");
           
        }

        public override object this[string propertyName]
        {
            get
            {
                return base[propertyName];
            }
        }
    }
}
