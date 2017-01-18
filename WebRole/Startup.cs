using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(ChumBucket.Startup))]
namespace ChumBucket {
    public partial class Startup {
        public void Configuration(IAppBuilder app) {
            // Nothing!
        }
    }
}
