using CommandLine;

namespace Kafka.Investigator.Tool.Attributes
{
    public class SubVerbAttribute : VerbAttribute
    {
        public Type BaseVerb { get; set; }
        public SubVerbAttribute(Type baseVerb, string name, bool isDefault = false, string[] aliases = null) : base(name, isDefault, aliases)
        {
            BaseVerb = baseVerb;
        }
    }
}
