
namespace Trafodion.Data
{
    using System.Reflection;
    using System.Resources;

    /// <summary>
    /// Collection of static methods used to interact with bundled resources.
    /// </summary>
    internal class TrafDbResources
    {
        private static readonly ResourceManager Messages = new ResourceManager("Trafodion.Data.Properties.Messages", Assembly.GetExecutingAssembly());

        internal static string FormatMessage(TrafDbMessage msg, params object [] p)
        {
            return string.Format(TrafDbResources.Messages.GetString(msg.ToString()), p);
        }

        internal static string GetMessage(TrafDbMessage msg)
        {
            return TrafDbResources.Messages.GetString(msg.ToString());
        }

        /*internal static string GetMessage(string msg)
        {
            return HPDbResources.Messages.GetString(msg);
        }*/
    }
}