
namespace EsgynDB.Data
{
    using System.Reflection;
    using System.Resources;

    /// <summary>
    /// Collection of static methods used to interact with bundled resources.
    /// </summary>
    internal class EsgynDBResources
    {
        private static readonly ResourceManager Messages = new ResourceManager("EsgynDB.Data.Properties.Messages", Assembly.GetExecutingAssembly());

        internal static string FormatMessage(EsgynDBMessage msg, params object [] p)
        {
            return string.Format(EsgynDBResources.Messages.GetString(msg.ToString()), p);
        }

        internal static string GetMessage(EsgynDBMessage msg)
        {
            return EsgynDBResources.Messages.GetString(msg.ToString());
        }
    }
}