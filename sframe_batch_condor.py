#!/usr/bin/env python


"""Submit SFrame jobs to the HTCondor batch system."""


import os
import sys
from argparse import ArgumentParser
import logging
# from lxml import etree as ET
import xml.etree.ElementTree as ET
import timeit
import subprocess
import shutil
from itertools import izip_longest

import job_conf_classes as jcc


fmt = '%(module)s.%(funcName)s():%(lineno)d >> %(message)s'
logging.basicConfig(level=logging.INFO, format=fmt)
log = logging.getLogger(__name__)


def grouper(iterable, n, fillvalue=None):
    """Collect data into fixed-length chunks or blocks
    Stolen from: https://docs.python.org/2/library/itertools.html#recipes

    e.g.
    >>> grouper('ABCDEFG', 3, 'x')
    ['ABC', 'DEF', 'Gxx']
    """
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


class BatchParser(ArgumentParser):
    """Class to handle commandline args"""

    def __init__(self, *args, **kwargs):
        super(BatchParser, self).__init__(*args, **kwargs)
        self.add_argument("filename", 
                          help="XML config to submit")
        self.add_argument("-w", "--workdir", default=None, 
                          help="Specify directory to store auxiliary files. Overrides whatever is set in <ConfigSGE>")
        self.add_argument("-v", "--verbose", action='store_true',
                          help="Display more messages")


def sanitise_path(filepath):
    """Resolve symlinks, and form absolute path.

    I can never remember which bit of os.path to use.
    """
    return os.path.realpath(filepath)


def get_expanded_xml(filename):
    """Returning full contents of XML file with expanded Entities.

    xmllint is used because the standard elementtree doesn't do external entity
    expansion for security reasons. lxml does, but isn't included with CMSSW 8.
    The easiest way therefore is to use xmllint to do this, then later on parse
    the expanded XML string.

    Returns
    -------
    str

    Raises
    ------
    IOError
        If input file does not exist

    subprocess.CalledProcessError
        If the call to xmllint fails
    """
    if not os.path.isfile(filename):
        raise IOError("No such XML file", filename)

    cmd = 'xmllint --noent %s' % filename
    expanded_contents = subprocess.check_output(cmd, shell=True)
    return expanded_contents


def get_batch_settings(xml_contents):
    """Get user's batch job settings from the raw XML string

    Not particularly nice, but don't want to break backward-compatibility.

    Parameters
    ----------
    xml_contents : str

    Returns
    -------
    dict

    Raises
    ------
    RuntimeError
        If multiple lines with ConfigParse or ConfigSGE tags
        If missing ConfigParse and/or ConfigSGE tags
    """
    found_parser_settings = False
    parser_settings_trigger = "<ConfigParse"

    found_batch_settings = False
    batch_settings_trigger = "<ConfigSGE"

    parser_settings_dict = {}
    batch_settings_dict = {}

    for line in xml_contents.splitlines():
        # log.debug(line)

        if parser_settings_trigger in line:
            if found_parser_settings:
                raise RuntimeError("Already processed a line with %s" % parser_settings_trigger)
            found_parser_settings = True

            parser_tree = ET.fromstring(line.strip())
            parser_settings_dict = dict(parser_tree.attrib)

        elif batch_settings_trigger in line:
            if found_batch_settings:
                raise RuntimeError("Already processed a line with %s" % parser_batch_trigger)
            found_batch_settings = True

            batch_tree = ET.fromstring(line.strip())
            batch_settings_dict = dict(batch_tree.attrib)

        if "<JobConfiguration" in line:
            break

    if not found_parser_settings:
        raise RuntimeError("Cannot find line with %s - did you include it?" % parser_settings_trigger)

    if not found_batch_settings:
        raise RuntimeError("Cannot find line with %s - did you include it?" % batch_settings_trigger)

    return parser_settings_dict, batch_settings_dict


def update_args(args, parser_settings, batch_settings):
    """Update the args with settings from the XML

    Parameters
    ----------
    args : argparse.Namespace
    parser_settings, batch_settings : dict
    """
    if args.workdir == None:
        args.workdir = sanitise_path(batch_settings['Workdir'])
    else:
        args.workdir = sanitise_path(args.workdir)
        log.info("Ignoring Workdir attribute in XML file, using %s instead", args.workdir)


def create_batch_workdir(workdir):
    """Create a workdir for batch/job files"""
    if os.path.isdir(workdir):
        log.warning("%s already exists - you probably want to clean it out first", workdir)
    else:
        log.debug("Creating workdir %s", workdir)
        os.makedirs(workdir)
        shutil.copy('JobConfig.dtd', workdir)


def store_tree(tree):
    """Store the XML as python objects

    Parameters
    ----------
    tree : xml.etree.ElementTree.Element

    Return
    ------
    job_conf_classes.JobConfiguration

    """    
    jc = jcc.JobConfiguration(library="", package="", **tree.attrib)

    # iterate over each Cycle element, store info into objects
    for cycle_ele in tree.findall('./Cycle'):

        cycle = jcc.Cycle(**cycle_ele.attrib)

        # store input data
        for id_ele in cycle_ele.findall('InputData'):
            this_id = jcc.InputData(**id_ele.attrib)

            for in_ele in id_ele.findall('In'):
                this_id.input_obj.append(jcc.In(**in_ele.attrib))

            in_tree_ele = id_ele.find('InputTree')
            if in_tree_ele is not None: 
                this_id.InputTree = jcc.InputTree(**in_tree_ele.attrib)            

            out_tree_ele = id_ele.find('OutputTree')
            if out_tree_ele is not None: 
                this_id.OutputTree = jcc.OutputTree(**out_tree_ele.attrib)            

            cycle.input_datas.append(this_id)

        # store userconfig
        uc_ele = cycle_ele.find('UserConfig')
        uc_items = [jcc.Item(**item_ele.attrib) for item_ele in uc_ele]
        user_config = jcc.UserConfig(uc_items)
        cycle.user_config = user_config

        jc.cycles.append(cycle)

    return jc


def main(in_args):
    """Main function"""

    parser = BatchParser(description=__doc__)
    args = parser.parse_args(in_args)

    if args.verbose:
        log.setLevel(logging.DEBUG)

    args.filename = sanitise_path(args.filename)
    log.info("Processing %s" % args.filename)

    xml_str = get_expanded_xml(args.filename)
    
    parser_settings, batch_settings = get_batch_settings(xml_str)
    
    log.debug('parser_settings: %s', parser_settings)
    log.debug('batch_settings: %s', batch_settings)

    update_args(args, parser_settings, batch_settings)

    tree = ET.fromstring(xml_str)
    # log.debug(ET.tostring(tree))

    create_batch_workdir(args.workdir)

    job_config = store_tree(tree)
    log.debug(job_config)

    # create XML files for each job, and all necessary folders

    # create condor job file per job? or altogether?

    # submit jobs, and keep some record of jobs, IDs, output file locations, status
    
    return 0


if __name__ == "__main__":
    start = timeit.default_timer()
    status = main(sys.argv[1:])
    stop = timeit.default_timer()
    log.info("Ran in %.3f seconds", stop - start)
    exit(status)
