﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace ChumBucket.Controllers {
    public class ApplicationController : Controller {
        public ActionResult Index() {
            return View();
        }

        public ActionResult Analysis() {
            return View();
        }
    }
}