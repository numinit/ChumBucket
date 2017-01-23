using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace WebRole.Controllers {
    public class ToolsController : Controller {
        public ActionResult Upload() {
            return View();
        }

        public ActionResult Analysis() {
            return View();
        }
    }
}