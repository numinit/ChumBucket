using Microsoft.Owin;
using Owin;
using WebRole;

[assembly: OwinStartupAttribute(typeof(ChumBucket.Startup))]
namespace ChumBucket {
    public partial class Startup {
        public void Configuration(IAppBuilder app) {
            AzureConfig.CreateClients();
        }
    }
}
