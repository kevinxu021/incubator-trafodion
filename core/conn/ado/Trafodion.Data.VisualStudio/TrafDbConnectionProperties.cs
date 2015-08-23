using System;
using Microsoft.VisualStudio.Data.AdoDotNet;

namespace Trafodion.Data.VisualStudio
{
	public class TrafDbConnectionProperties : AdoDotNetConnectionProperties
	{
        public TrafDbConnectionProperties(): 
            base("Trafodion.Data")
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
