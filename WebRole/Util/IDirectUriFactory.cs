using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace WebRole.Util {
    public interface IDirectUriFactory {
        /**
         * Builds a direct URI to a resource given a bucket and a key.
         * <param name="bucket">The bucket name</param>
         * <param name="key">The key</param>
         */
        Uri BuildDirectUri(string bucket, string key = null);

        /**
         * Builds a direct HTTPS URI to a resource given a bucket and a key.
         * <param name="bucket">The bucket name</param>
         * <param name="key">The key</param>
         */
        Uri BuildDirectHttpsUri(string bucket, string key = null);
    }
}