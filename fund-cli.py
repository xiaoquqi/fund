#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Usage:
#     ./fund-cli.py -d -v
#

"""Python script contains logging setup and argparse"""

import argparse
import logging
import os
import sys

from fund import FundRank

CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
SCRIPT_NAME = os.path.basename(__file__)


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
        description="Get real time fund list")
    parser.add_argument(
        "-d", "--debug", action="store_true", dest="debug",
        default=False, help="Enable debug message.")
    parser.add_argument(
        "-v", "--verbose", action="store_true", dest="verbose",
        default=False, help="Show message in standard output.")

    return parser.parse_args(argv[1:])


def main():
    args = parse_sys_args(sys.argv)
    log_init(CURRENT_PATH, SCRIPT_NAME, args.debug, args.verbose)

    fund_rank = FundRank(ft="gp")
    fund_rank_list = fund_rank.list()
    with open("fund-result.csv", "w") as fh:
        for line in fund_rank_list:
            #if type(line) is not str:
            #    print(line)
            #    print(type(line.encode("ascii", "ignore")))
            #    line = line.encode("ascii", "ignore")
            print(line)
            if type(line) is not str:
                line = line.encode("utf8")
            fh.write(line + "\n")

if __name__ == "__main__":
    main()
