#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Usage:
#     ./fund-collector.py -d -v
#

"""Python script contains logging setup and argparse"""

import argparse
import logging
import os
import sys

import pandas as pd

from eastmoney import FundInfo, FundRank, FundTsData

CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
SCRIPT_NAME = os.path.basename(__file__)

FUNDRANK_CSV = "fundrank.csv"
FUNDINFO_CSV = "fundinfo.csv"
FUNDTS_CSV = "fundts.csv"


def log_init(root_path, name="app", debug=False, verbose=False):
    """Set up global logs"""
    log_format = "%(asctime)s %(process)s %(levelname)s [-] %(message)s"
    log_level = logging.INFO
    log_path = os.path.join(root_path, "log")
    if not os.path.exists(log_path):
        os.makedirs(log_path)

    if debug:
        log_level = logging.DEBUG
        log_file = os.path.join(log_path, "%s.debug.log" % name)
    else:
        log_file = os.path.join(log_path, "%s.log" % name)

    if verbose:
        logging.basicConfig(
                format=log_format,
                level=log_level)
    else:
        logging.basicConfig(
                format=log_format,
                level=log_level,
                filename=log_file)


def parse_sys_args(argv):
    """Parses commaond-line arguments"""
    parser = argparse.ArgumentParser(
        description="Fund collect and analysis")
    parser.add_argument(
        "-d", "--debug", action="store_true", dest="debug",
        default=False, help="Enable debug message.")
    parser.add_argument(
        "-v", "--verbose", action="store_true", dest="verbose",
        default=False, help="Show message in standard output.")

    subparsers = parser.add_subparsers(title="Avaliable commands")

    collect_parser = subparsers.add_parser("collect")
    collect_parser.set_defaults(func=fund_collect)

    analysis_parser = subparsers.add_parser("analysis")
    analysis_parser.set_defaults(func=fund_analysis)

    return parser.parse_args(argv[1:])

def fund_collect(args):
    fundrank = FundRank(ft="gp")
    fundrank_dt = fundrank.list()
    fundrank_dt.to_csv(FUNDRANK_CSV)

    fund_codes = [f for f in fundrank_dt["code"]]
    fundinfo = FundInfo(fund_codes)
    fundinfo_dt = fundinfo.list()
    fundinfo_dt.to_csv(FUNDINFO_CSV)

    fundts = FundTsData(fund_codes)
    fundts_dt = fundts.list()
    fundts_dt.to_csv(FUNDTS_CSV)

def fund_analysis(args):
    dtype_dict = {"code": str}
    fundinfo_dt = pd.read_csv(FUNDINFO_CSV, dtype=dtype_dict)
    fund_dt = fundinfo_dt

    fundrank_dt = pd.read_csv(
        FUNDRANK_CSV, dtype=dtype_dict,
        usecols=["code", "近3年", "近2年", "近1年", "近6月", "近3月"])
    fund_dt = pd.merge(fund_dt, fundrank_dt, on="code")

    fundts_dt = pd.read_csv(FUNDTS_CSV, dtype=dtype_dict)
    fund_dt = pd.merge(fund_dt, fundts_dt, on="code")

    # add weight
    fund_dt["rank"] = fund_dt["近3年"] * 0.3 + \
                      fund_dt["近2年"] * 0.25 + \
                      fund_dt["近1年"] * 0.2 + \
                      fund_dt["近6月"] * 0.15 + \
                      fund_dt["近3月"] * 0.1
    fund_dt.sort_values(by="rank", inplace=True, ascending=False)

    fund_dt.to_csv("report_all.csv")
    fund_dt = fund_dt[0:]

    fund_dt.to_csv("report_50.csv")

    fund_dt.sort_values(
        by=["夏普比率近三年", "夏普比率近二年", "夏普比率近一年"],
        ascending=False,
        inplace=True)

    print(fund_dt[0:20])
    fund_dt.to_csv("report.csv")

def main():
    args = parse_sys_args(sys.argv)
    log_init(CURRENT_PATH, SCRIPT_NAME, args.debug, args.verbose)
    args.func(args)

if __name__ == "__main__":
    main()
