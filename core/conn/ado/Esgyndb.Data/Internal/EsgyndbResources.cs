
namespace Esgyndb.Data
{
    using System.Reflection;
    using System.Resources;

    /// <summary>
    /// Collection of static methods used to interact with bundled resources.
    /// </summary>
    internal class EsgyndbResources
    {
        private static readonly ResourceManager Messages = new ResourceManager("Esgyndb.Data.Properties.Messages", Assembly.GetExecutingAssembly());

        internal static string FormatMessage(EsgyndbMessage msg, params object [] p)
        {
            return string.Format(EsgyndbResources.Messages.GetString(msg.ToString()), p);
        }

        internal static string GetMessage(EsgyndbMessage msg)
        {
            return EsgyndbResources.Messages.GetString(msg.ToString());
        }
    }
}