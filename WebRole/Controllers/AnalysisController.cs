using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace WebRole.Controllers {
    [RoutePrefix("analysis")]
    public class AnalysisController : Controller {
        [HttpPost]
        [Route("submit")]
        public ActionResult Submit() {
            return View();
        }

        [HttpGet]
        [Route("status")]
        public ActionResult Status() {
            return View();
        }

        [HttpGet]
        [Route("result")]
        public ActionResult Result() {
            return View();
        }
    }
}