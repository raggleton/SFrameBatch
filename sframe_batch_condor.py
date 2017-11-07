#!/usr/bin/env python


"""Submit SFrame jobs to the HTCondor batch system."""


import os
import sys
from argparse import ArgumentParser
import logging
import xml.etree.ElementTree as ET
import timeit
import subprocess
import shutil
from copy import deepcopy
from pprint import pformat

import job_conf_classes as jcc
from local_manager import Manager
from utils import sanitise_path, sort_nicely


fmt = '%(module)s.%(funcName)s:%(lineno)d >> %(message)s'
logging.basicConfig(level=logging.INFO, format=fmt)
# logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class BatchParser(ArgumentParser):
    """Class to handle commandline args"""

    def __init__(self, *args, **kwargs):
        super(BatchParser, self).__init__(*args, **kwargs)
        self.add_argument("filename",
                          help="SFrame XML config")
        self.add_argument("-s", "--submit", action='store_true',
                          help="Submit jobs. If workdir does not exist, "
                          "this will be the default action.")
        self.add_argument("-w", "--workdir", default=None,
                          help="Specify directory to store auxiliary files. "
                               "Overrides whatever is set in <ConfigSGE>")
        self.add_argument("--dry", action='store_true',
                          help="Dry run: create all files, but do not submit jobs")
        self.add_argument("-v", "--verbose", action='store_true',
                          help="Display more messages")


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
        User args from CL
    parser_settings : dict
        Setting from XML
    batch_settings : dict
        Batch setting from XML
    """
    # Make sure to use the command line workdir over the file one
    # and account for the lower vs upper case difference
    if args.workdir is not None:
        batch_settings['workdir'] = args.workdir
    else:
        batch_settings['workdir'] = batch_settings['Workdir']

    args.__dict__.update(parser_settings)
    args.__dict__.update(batch_settings)


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
                this_id.input_tree = jcc.InputTree(**in_tree_ele.attrib)

            out_tree_ele = id_ele.find('OutputTree')
            if out_tree_ele is not None:
                this_id.output_tree = jcc.OutputTree(**out_tree_ele.attrib)

            cycle.input_datas.append(this_id)

        # store userconfig
        uc_ele = cycle_ele.find('UserConfig')
        uc_items = [jcc.Item(**item_ele.attrib) for item_ele in uc_ele]
        user_config = jcc.UserConfig(uc_items)
        cycle.user_config = user_config

        jc.cycles.append(cycle)

    return jc


def process_cycle(cycle, args, template_root):
    """Handle a Cycle: setup all the jobs and files needed for submission

    Parameters
    ----------
    cycle : job_conf_classes.Cycle
        Overall Cycle element
    args : ArgumentParser.Namespace
        User args about job configuration etc
    template_root : ElementTree.Element
        Template JobConfiguration Element to give to jobs to create XML files
    """
    manager = Manager(cycle)
    manager.setup_datasets()
    manager.setup_jobs(args=args)
    manager.write_batch_files(template_root)
    return manager


def create_template_root(tree):
    """Create a template JobConfiguration Element for jobs to customise

    Parameters
    ----------
    tree : ElementTree.Element
        Original JobConfiguration element to use as a template

    Returns
    -------
    ElementTree.Element
        New template with no InputData elements
    """
    new_tree = deepcopy(tree)
    cycle = new_tree.find("Cycle")
    if cycle is None:
        raise RuntimeError("Cannot find JobCycle element")
    for id_ele in cycle.findall("InputData"):
        cycle.remove(id_ele)
    return new_tree


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
    log.debug(args)

    if not os.path.isdir(args.workdir):
        args.submit = True
    else:
        if args.submit:
            log.warning("%s already exists! No jobs submitted - doing job monitoring instead", args.workdir)
            args.submit = False

    if args.submit:
        # Create and submit jobs
        log.info("Creating workdir %s", args.workdir)
        os.makedirs(args.workdir)

        tree = ET.fromstring(xml_str)

        job_config = store_tree(tree)

        template_root = create_template_root(tree)

        for cycle in job_config.cycles:
            manager = process_cycle(cycle, args, template_root)
            if not args.dry:
                manager.submit_jobs()
            else:
                for dataset in manager.datasets:
                    log.info('condor_submit %s', dataset.job_file)
    else:
        log.info("Jobs submitted - processing status for workdir %s", args.workdir)
        # look for JSON status files to reconstruct objects
        manager = Manager()
        dataset_dirs = []
        for item in os.listdir(args.workdir):
            full_path = os.path.join(args.workdir, item)
            if not os.path.isdir(full_path):
                continue

            status_json = os.path.join(full_path, "status.json")
            if os.path.isfile(status_json):
                dataset_dirs.append(full_path)

        sort_nicely(dataset_dirs)

        # Recreate Datasets
        for dset_dir in dataset_dirs:
            status_json = os.path.join(dset_dir, "status.json")
            manager.load_dataset_from_json(status_json)

        # Update info, print status
        manager.update_dataset_statuses()
        manager.display_progress()

        if len(dataset_dirs) == 0:
            raise RuntimeError("No status JSONs - if no jobs running then delete this workdir and try again")

    return 0


if __name__ == "__main__":
    start = timeit.default_timer()
    status = main(sys.argv[1:])
    stop = timeit.default_timer()
    log.info("Ran in %.3f seconds", stop - start)
    exit(status)
