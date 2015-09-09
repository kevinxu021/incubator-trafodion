using System;
using Microsoft.VisualStudio.Data.AdoDotNet;

namespace Esgyndb.Data.VisualStudio
{
	public class EsgyndbConnectionProperties : AdoDotNetConnectionProperties
	{
        public EsgyndbConnectionProperties(): 
            base("Esgyndb.Data")
        {
        }

		public override bool IsComplete
		{
			get
			{
                return !(String.IsNullOrEmpty(this["Server"].ToString()) ||
                    String.IsNullOrEmpty(this["User"].ToString()) ||
                    String.IsNullOrEmpty(this["Password"].ToString()));
			}
		}
	}
}
