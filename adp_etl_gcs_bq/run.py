#!/usr/bin/env python
# coding: utf-8

import utils as U
import report_handler

config = U.get_config("config.ini")

r_obj = report_handler.ReportHandler(config)
r_obj.run_report_handler()