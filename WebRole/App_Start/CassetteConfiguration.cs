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
            // TODO: Configure your bundles here...
            // Please read http://getcassette.net/documentation/configuration
            bundles.AddPerIndividualFile<StylesheetBundle>("Stylesheets");
            //bundles.Add<StylesheetBundle>("chumbucket-ext", "Stylesheets/chumbucket-ext.css");
            bundles.AddPerIndividualFile<ScriptBundle>("Scripts");
            //bundles.Add<ScriptBundle>("chumbucket-ext", "Scripts/chumbucket-ext.js");
        }
    }
}