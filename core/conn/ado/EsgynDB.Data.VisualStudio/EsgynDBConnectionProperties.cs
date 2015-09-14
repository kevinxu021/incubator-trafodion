using System;
using Microsoft.VisualStudio.Data.AdoDotNet;

namespace EsgynDB.Data.VisualStudio
{
	public class EsgynDBConnectionProperties : AdoDotNetConnectionProperties
	{
        public EsgynDBConnectionProperties(): 
            base("EsgynDB.Data")
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
