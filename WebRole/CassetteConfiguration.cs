using Cassette;
using Cassette.Scripts;
using Cassette.Stylesheets;
using System.IO;
using System.Text.RegularExpressions;

namespace WebRole {
    /// <summary>
    /// Configures the Cassette asset bundles for the web application.
    /// </summary>
    public class CassetteBundleConfiguration : IConfiguration<BundleCollection> {
        public void Configure(BundleCollection bundles) {
            bundles.AddPerIndividualFile<StylesheetBundle>("Stylesheets");
            bundles.AddPerIndividualFile<ScriptBundle>("Scripts");
        }
    }
}