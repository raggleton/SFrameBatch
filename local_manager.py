"""Classes & functions to handle jobs locally (as opposed to on batch system)."""


import os
from utils import dict_to_str, grouper
import logging
from copy import deepcopy
import xml.etree.ElementTree as ET
import subprocess

from utils import sanitise_path


fmt = '%(module)s.%(funcName)s:%(lineno)d >> %(message)s'
logging.basicConfig(level=logging.INFO, format=fmt)
# logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


import ROOT
ROOT.PyConfig.IgnoreCommandLineOptions = True


def get_num_events(filename, tree_name):
    """Return number of events in tree `tree_name` in ROOT file `filename`

    Parameters
    ----------
    filename : str
        ROOT filename
    tree_name : str
        Name of TTree

    Returns
    -------
    int
        Number of events in tree

    Raises
    ------
    RuntimeError
        If file doens't contain a TTree named tree_name
    """
    return 1 # ONLY FOR TESTING
    n = 0
    try:
        f = ROOT.TFile(filename)
        n = f.Get(tree_name).GetEntriesFast()
    except:
        raise RuntimeError('%s does not contain %s' % (filename, tree_name))
    finally:
        f.Close()
        return n


def generate_sframe_filename(inputdata_type, inputdata_version):
    """Determine output filename like in SFrame.

    Parameters
    ----------
    inputdata_type : str
        Type attribute of <InputData>
    inputdata_version : str
        Version attribute of <InputData>

    Returns
    -------
    str
    """
    return ".".join(['uhh2.AnalysisModuleRunner', inputdata_type, inputdata_version, "root"])


class Job(object):

    """Handle info about a single Job within a Dataset

    Attributes
    ----------
    input_files : [str]
        Name of input files
    job_index : int
        Index of Job within its Dataset, not a global index
    nevents_max : int
        Number of events to process
    nevents_skip : int
        Number of events to skip processing
    output_file : str
        Name of output ROOT file
    xml_filename : str
        Name of XML file for SFrame
    """

    def __init__(self, job_index, input_files=None, output_file=None, nevents_max=-1, nevents_skip=0, xml_filename=""):
        self.job_index = job_index
        self.input_files = input_files or []
        self.output_file = output_file
        self.nevents_max = nevents_max
        self.nevents_skip = nevents_skip
        self.xml_filename = xml_filename

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def customise_xml_contents(self, template_root):
        """Given a template, customise the InputData element with this Job's files and settings

        Parameters
        ----------
        template_root : ElementTree.Element
            Template JobConfiguration Element to customise
        """
        cycle = template_root.find('Cycle')
        cycle.set('PostFix', "_%d" % self.job_index)

        input_data_element = cycle.find('InputData')
        input_data_element.set('NEventsMax', str(self.nevents_max))
        input_data_element.set('NEventsSkip', str(self.nevents_skip))

        # remove any old files
        for in_element in input_data_element.findall("In"):
            input_data_element.remove(in_element)

        # add files for this job
        for f in self.input_files:
            in_ele = ET.SubElement(input_data_element, "In")
            in_ele.set("FileName", f.filename)
            in_ele.set("Lumi", str(f.lumi))


    def write_xml_file(self, template_root):
        """Write XML file for this Job, using a template XML Element

        Parameters
        ----------
        template_root : ElementTree.Element
            JobConfiguration element to update
        """
        self.customise_xml_contents(template_root)

        # log.debug(ET.tostring(template_root))

        xml_header = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE JobConfiguration PUBLIC "" "JobConfig.dtd">
"""
        with open(self.xml_filename, 'w') as fout:
            log.debug('Writing XML to %s', self.xml_filename)
            fout.write(xml_header)
            fout.write("\n")
            fout.write(ET.tostring(template_root, encoding="UTF-8", method="html"))
            fout.write("\n")


class File(object):

    """Bare-bones class to hold info about an input file

    TODO: Just update job_conf_classes.In ? Virtually no difference

    Attributes
    ----------
    filename : str
        Path of file
    nevents : int
        Number of events in file
    lumi : float
        Description
    """

    def __init__(self, filename, nevents, lumi):
        self.filename = filename
        self.nevents = int(nevents)
        self.lumi = float(lumi)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))


class Dataset(object):

    """Handle info about a group of input files making up a <InputData> element.

    Each Dataset will be submitted using 1 condor job file.

    Attributes
    ----------
    files : [File]
        Collection of Files that make up this Dataset
    final_file : str
        Final output filename
    input_data : job_conf_classes.InputData
        Reference InputData object
    job_batchdir : str
        Top directory for all job-related files
    job_batchdir_err : str
        Directory for all job STDERR files
    job_batchdir_log : str
        Directory for all job HTCondor log files
    job_batchdir_out : str
        Directory for all job STDOUT files
    job_batchdir_xml : str
        Directory for all job XML files
    job_file : str
        HTCondor job file
    job_outdir : str
        Output directory for per-Job ROOT files
    jobs : list
        Collection of Jobs for this Dataset
    name : str
        Name of this dataset (i.e. Version)

    """

    def __init__(self, input_data):
        self.input_data = input_data
        self.name = input_data.Version
        self.type = input_data.Type
        tree_name = input_data.input_tree.Name
        self.files = [File(filename=input_file.FileName,
                           nevents=get_num_events(input_file.FileName, tree_name),
                           lumi=0)
                      for input_file in input_data.input_obj]
        self.jobs = []
        self.final_file = None
        self.job_outdir = None
        self.job_batchdir = None
        self.job_batchdir_xml = None
        self.job_batchdir_out = None
        self.job_batchdir_err = None
        self.job_batchdir_log = None

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def setup_jobs_dirs(self, output_dir, workdir):
        """Create dirs for job outputs/condor files, and add in DTDs

        Parameters
        ----------
        output_dir : str
            Output directory for final ROOT file
        workdir : str
            Workdir name
        """
        # Setup all the dirs needed for this dataset & its jobs
        self.job_outdir = os.path.join(output_dir, workdir)
        self.job_batchdir = os.path.join(workdir, self.name)
        self.job_batchdir_xml = os.path.join(self.job_batchdir, "xml")
        self.job_batchdir_out = os.path.join(self.job_batchdir, "out")
        self.job_batchdir_err = os.path.join(self.job_batchdir, "err")
        self.job_batchdir_log = os.path.join(self.job_batchdir, "log")
        self.final_file = sanitise_path(os.path.join(output_dir, generate_sframe_filename(self.type, self.name)))

        # Make sure output directories exist
        dirs = ['job_outdir',
                'job_batchdir_xml',
                'job_batchdir_out',
                'job_batchdir_err',
                'job_batchdir_log']
        for odir_name in dirs:
            odir = sanitise_path(getattr(self, odir_name))
            setattr(self, odir_name, odir)
            if not os.path.isdir(odir):
                os.makedirs(odir)

        # Link DTD file into XML dir
        dtd = "JobConfig.dtd"
        this_dtd = os.path.join(self.job_batchdir_xml, dtd)
        if not os.path.isfile(this_dtd):
            original_dtd = os.path.join(os.path.dirname(__file__), dtd)
            os.symlink(original_dtd, this_dtd)

    def group_files_into_jobs(self, splitting_mechanism="nfiles", splitting_value=1):
        """Turn the list of input files into a set of Jobs based on some splitting mechanism.

        Parameters
        ----------
        splitting_mechanism : str, optional
            Method for dividing up files into jobs
        splitting_value : int, optional
            Value for splitting

        Raises
        ------
        RuntimeError
            If splitting option not valid
        """

        if splitting_mechanism not in ['nfiles', 'nevents']:
            raise RuntimeError("%s is not a valid splitting option" % splitting_mechanism)

        if splitting_mechanism is "nfiles":
            for ind, file_group in enumerate(grouper(self.files, splitting_value, fillvalue=None)):
                ind_str = "_%d" % ind
                out_filename = os.path.join(self.job_outdir, generate_sframe_filename(self.type, self.name + ind_str))
                xml_filename = os.path.join(self.job_batchdir_xml, self.name + "_%d.xml" % ind)
                job = Job(job_index=ind,
                          input_files=[f for f in file_group if f],
                          output_file=out_filename,
                          xml_filename=xml_filename,
                          nevents_max=-1)
                log.debug(job)
                self.jobs.append(job)
        elif splitting_mechanism is "nevents":
            raise RuntimeError("nevents is not implemented yet!")

    def write_condor_files(self):
        """Write HTCondor job file for this Dataset"""
        template = """Executable = {EXE}
Universe = vanilla

rootname = $Fn(filename)

Output = {OUTFILE}
Error = {ERRFILE}
Log = {LOGFILE}

Should_Transfer_Files = NO

# Pass on our env vars
getenv = True

InitialDir = $ENV(PWD)

request_memory = 3GB

arguments = $(filename)
queue filename from {LISTFILE}
"""

        exe_script = os.path.join(os.path.dirname(__file__), "worker_run.sh")

        job_args = {
            "EXE": exe_script,
            "OUTFILE": os.path.join(self.job_batchdir_out, "$(rootname).$(cluster).$(process).out"),
            "ERRFILE": os.path.join(self.job_batchdir_err, "$(rootname).$(cluster).$(process).err"),
            "LOGFILE": os.path.join(self.job_batchdir_log, "$(rootname).$(cluster).$(process).log"),
            "LISTFILE": os.path.join(self.job_batchdir, "xml_list.txt")
        }
        file_contents = template.format(**job_args)

        self.job_file = os.path.join(self.job_batchdir, self.name + ".job")
        with open(self.job_file, 'w') as f:
            f.write(file_contents)

        # write file with list of filenames
        # we do it this way to ensure the job index = $(process),
        # otherwise it would go _1.xml, _10.xml, _100.xml etc
        # and render the naming useless
        with open(job_args['LISTFILE'], 'w') as f:
            for j in self.jobs:
                f.write(j.xml_filename+'\n')

    def write_xml_files(self, template_root):
        """Get all Jobs to write their XML files using a template Element

        This will update the template for this dataset,
        and create a single InputData element that each Job should fill
        with necessary In elements, and update attributes like NEventsMax, etc

        Parameters
        ----------
        template_root : ElementTree.Element
            JobConfiguration Element that each Job uses as a template.
        """
        this_template = deepcopy(template_root)
        # Update Cycle: Remove all existing InputData elements,
        # update output location
        cycle = this_template.find('Cycle')
        cycle.set("OutputDirectory", self.job_outdir+"/")  # ending / important!
        for input_data_ele in cycle.findall("InputData"):
            cycle.remove(input_data_ele)

        # add a dummy InputData element
        # each Job will then fill it with its files and settings
        input_data = ET.Element("InputData")
        for attr in ['Type', 'Version', 'Lumi', 'NEventsMax', 'NEventsSkip', 'Cacheable', 'SkipValid']:
            input_data.set(attr, str(getattr(self.input_data, attr)))
        # NB have to insert first as the DTD is expecting InputData THEN UserConfig
        cycle.insert(0, input_data)

        # Add in InputTree and OutputTree
        input_tree_element = ET.SubElement(input_data, "InputTree")
        input_tree_element.set("Name", self.input_data.input_tree.Name)

        if self.input_data.output_tree:
            output_tree_element = ET.SubElement(input_data, "OutputTree")
            output_tree_element.set("Name", self.input_data.output_tree.Name)

        # Each job should add their files, and write to file
        for job in self.jobs:
            job.write_xml_file(this_template)

    def submit_jobs(self):
        """Send jobs to the batch system."""
        cmd = "condor_submit " + self.job_file
        log.debug(cmd)
        subprocess.check_call(cmd, shell=True)


class Manager(object):
    """Manage all Dataset corresponding to a XML file

    Attributes
    ----------
    job_cycle : Cycle
        Master Cycle element, holding info about the datasets
    input_datasets : list
        Hold list of all Dataset objects for this XML file
    """

    def __init__(self, job_cycle):
        self.job_cycle = job_cycle
        self.input_datasets = []

        self.setup_datasets()

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    @property
    def jobs(self):
        for dataset in self.input_datasets:
            for job in dataset.jobs:
                yield job

    def setup_datasets(self):
        """Create Datasets from InputData elements in the JobCycle"""
        for input_data in self.job_cycle.input_datas:
            dataset = Dataset(input_data)
            self.input_datasets.append(dataset)

    def setup_jobs(self, args):
        """Make Datasets divide up files into Jobs, and create necessary dirs

        Parameters
        ----------
        args : argparse.Namespace
            User args
        """
        # figure out how the user want to split things
        splitting_mechanism = ""
        splitting_value = 0

        if int(args.NEventsBreak) != 0:
            splitting_mechanism = "nevents"
            splitting_value = int(args.NEventsBreak)
            log.info('Splitting into jobs: %d events / job', splitting_value)
            # FIXME how to handle 'LastBreak' ?
            log.warning('Ignoring LastBreak attribute as not implemented')

        if int(args.FileSplit) != 0:
            if splitting_mechanism != "":
                raise RuntimeError("You cannot specify both NEventsBreak and FileSplit in <ConfigParse>.")
            splitting_mechanism = "nfiles"
            splitting_value = int(args.FileSplit)
            log.info('Splitting into jobs: %d files / job', splitting_value)

        total_jobs = 0
        for dataset in self.input_datasets:
            dataset.setup_jobs_dirs(self.job_cycle.OutputDirectory, args.workdir)
            dataset.group_files_into_jobs(splitting_mechanism, splitting_value)

            total_jobs += len(dataset.jobs)
            log.info('%s => %d jobs', dataset.name, len(dataset.jobs))

        log.info("TOTAL: %d jobs", total_jobs)
        log.debug(self.input_datasets)

    def write_batch_files(self, template_root):
        """Make all Datasets write all files necessary for batch jobs.

        This includes both HTCondor job files, and SFrame XML files.
        The template is for XMl files.

        Parameters
        ----------
        template_root : ElementTree.Element
            JobConfiguration element, used as a template for XML files

        """
        for dataset in self.input_datasets:
            dataset.write_condor_files()
            dataset.write_xml_files(template_root)

    def submit_jobs(self):
        """Submit all jobs across all Datasets."""
        log.info('Submitting jobs')
        for dataset in self.input_datasets:
            log.info(dataset.name)
            dataset.submit_jobs()
