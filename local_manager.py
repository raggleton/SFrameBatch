"""Classes & functions to handle jobs locally (as opposed to on batch system)."""


import os
from utils import dict_to_str, grouper
import logging
from copy import deepcopy
import xml.etree.ElementTree as ET
import subprocess
from glob import glob
import json

from utils import sanitise_path
import job_conf_classes


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
    cluster : int
        HTcondor job Cluster number
    input_files : [str]
        Name of input files
    job_index : int
        Index of Job within its Dataset, not a global index
    log_filename : str
        HTCondor log filename
    nevents_max : int
        Number of events to process
    nevents_skip : int
        Number of events to skip processing
    output_file : str
        Name of output ROOT file
    process : int
        HTcondor job Process number
    status : int
        Job status
    stderr_filename : str
        File for STDERR
    stdout_filename : str
        File for STDOUT
    xml_filename : str
        Name of XML file for SFrame
    """

    def __init__(self, job_index, input_files=None, output_file=None, nevents_max=-1, nevents_skip=0,
                 xml_filename="", stdout_filename="", stderr_filename="", log_filename="",
                 cluster=0, process=0, status=-1):
        self.job_index = job_index
        self.input_files = input_files or []
        self.output_file = output_file
        self.nevents_max = nevents_max
        self.nevents_skip = nevents_skip
        self.xml_filename = xml_filename
        self.stdout_filename = stdout_filename
        self.stderr_filename = stderr_filename
        self.log_filename = log_filename
        self.cluster = int(cluster)
        self.process = int(process)
        self.status = int(status)

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

    def get_cluster_process(self):
        """Figure out cluster and process numbers from log file"""
        with open(self.log_filename) as f:
            line = f.readline()
        info = line.split()[1].lstrip("(").rstrip(")").split(".")
        self.cluster = int(info[0])
        self.process = int(info[1])

    def update_stdout_stderr_filenames(self, stdout_template, stderr_template):
        """Update STDOUT/STDERR filenames using cluster & process

        Parameters
        ----------
        stdout_template : str
            STDOUT filename template. Must have $(cluster) and $(process)
        stderr_template : str
            STDERR filename template. Must have $(cluster) and $(process)
        """
        self.stdout_filename = stdout_template.replace("$(process)", str(self.process)).replace("$(cluster)", str(self.cluster))
        self.stderr_filename = stderr_template.replace("$(process)", str(self.process)).replace("$(cluster)", str(self.cluster))


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
    input_files : [File]
        Collection of Files that make up this Dataset
    final_file : str
        Final output filename
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
    log_stem : str
        Template filename for HTCondor log files
    name : str
        Name of this dataset (i.e. Version)
    stderr_stem : str
        Template filename for STDOUT files
    stdout_stem : str
        Template filename for STDERR files
    type : str
        Type attribute from InputData tag

    """

    def __init__(self):
        self.name = None
        self.type = None
        self.input_files = []
        self.jobs = []
        self.final_file = None
        self.job_outdir = None
        self.job_batchdir = None
        self.job_batchdir_xml = None
        self.job_batchdir_out = None
        self.job_batchdir_err = None
        self.job_batchdir_log = None
        self.status_json = None

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def setup_from_input_data(self, input_data):
        """Setup Dataset from InputData object

        Parameters
        ----------
        input_data : job_conf_classes.InputData
            Reference object
        """
        self.name = input_data.Version
        self.type = input_data.Type
        tree_name = input_data.input_tree.Name
        self.input_files = [File(filename=input_file.FileName,
                                 nevents=get_num_events(input_file.FileName, tree_name),
                                 lumi=0)
                            for input_file in input_data.input_obj]
        self.input_data = input_data

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
        self.status_json = os.path.join(self.job_batchdir, "status.json")
        # Each job puts its out/err/log files in a job-specific dir
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
            for ind, file_group in enumerate(grouper(self.input_files, splitting_value, fillvalue=None)):
                ind_str = "_%d" % ind
                out_filename = os.path.join(self.job_outdir, generate_sframe_filename(self.type, self.name + ind_str))
                xml_filename = os.path.join(self.job_batchdir_xml, self.name + "_%d.xml" % ind)
                # placeholder filename for STDOUT/ERR/log - will be replaced after jobs actually submitted, but makes life easier
                stem = os.path.join(self.job_batchdir, self.name)
                job = Job(job_index=ind,
                          input_files=[f for f in file_group if f],
                          output_file=out_filename,
                          xml_filename=xml_filename,
                          nevents_max=-1,
                          stdout_filename=stem+".out",
                          stderr_filename=stem+".err",
                          log_filename=stem+".log")
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

JobBatchName = {JOBNAME}

request_memory = 1GB

arguments = $(filename)
queue filename from {LISTFILE}
"""

        exe_script = os.path.join(os.path.dirname(__file__), "worker_run.sh")

        self.stdout_stem = os.path.join(self.job_batchdir_out, "$(rootname).$(cluster).$(process).out")
        self.stderr_stem = os.path.join(self.job_batchdir_err, "$(rootname).$(cluster).$(process).err")
        self.log_stem = os.path.join(self.job_batchdir_log, "$(rootname).$(cluster).$(process).log")

        job_args = {
            "EXE": exe_script,
            "OUTFILE": self.stdout_stem,
            "ERRFILE": self.stderr_stem,
            "LOGFILE": self.log_stem,
            "LISTFILE": os.path.join(self.job_batchdir, "xml_list.txt"),
            "JOBNAME": "%s_%s" % (os.path.basename(self.job_outdir), self.name)
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

    def find_job_logs(self):
        """Find the log file for each Job in the Dataset, and update STDOUT/ERR locations

        If more than 1 match for log file, use the newest.

        We do it from here, since Dataset controls the pattern for the OUT/ERR/log filenames

        TODO: I guess if we submitted via python API we'd get cluster/proc automatically
        """
        for job in self.jobs:
            num_glob = "[0-9]*"
            pattern = self.log_stem.replace("$(cluster)", num_glob).replace("$(process)", num_glob)
            xml_stem = os.path.splitext(os.path.basename(job.xml_filename))[0]
            pattern = pattern.replace("$(rootname)", xml_stem)
            matches = glob(pattern)
            if len(matches) == 0:
                log.warning("Cannot find log file matching %s", pattern)
                job.log_filename = ""
            elif len(matches) > 1:
                log.warning("Found more than 1 log file matching %s, using the newest created one", pattern)
                job.log_filename = max(matches, key=os.path.getctime)
            else:
                job.log_filename = matches[0]

            if job.log_filename == "":
                continue

            # Tell the Job to update itself
            job.get_cluster_process()
            new_stdout = self.stdout_stem.replace("$(rootname)", xml_stem)
            new_stderr = self.stderr_stem.replace("$(rootname)", xml_stem)
            job.update_stdout_stderr_filenames(new_stdout, new_stderr)

    def construct_status_dict(self):
        """Construct dict for this Dataset

        Returns
        -------
        dict
            Description
        """
        status_dict = {}
        # Only keep fields that aren't "deep" or unserializable
        iterables = [list, tuple, job_conf_classes.InputData]
        for k, v in self.__dict__.iteritems():
            if type(v) in iterables:
                continue
            status_dict[k] = v

        # Add in Jobs separately
        job_dicts = []
        for job in self.jobs:
            jd = {k:v for k, v in job.__dict__.iteritems() if type(v) not in iterables}
            job_dicts.append(jd)
        status_dict["jobs"] = job_dicts
        return status_dict

    def write_json_status(self):
        """Save Dataset status as JSON file"""
        status_dict = self.construct_status_dict()
        with open(self.status_json, "w") as f:
            json.dump(status_dict, f, indent=2)


class Manager(object):
    """Manage all Dataset corresponding to a XML file

    Attributes
    ----------
    job_cycle : Cycle
        Master Cycle element, holding info about the datasets
    datasets : list
        Hold list of all Dataset objects for this XML file
    """

    def __init__(self, job_cycle=None):
        self.job_cycle = job_cycle
        self.datasets = []

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    @property
    def jobs(self):
        for dataset in self.datasets:
            for job in dataset.jobs:
                yield job

    def setup_datasets(self):
        """Create Datasets from InputData elements in the JobCycle"""
        for input_data in self.job_cycle.input_datas:
            dataset = Dataset()
            dataset.setup_from_input_data(input_data)
            self.datasets.append(dataset)

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
        for dataset in self.datasets:
            dataset.setup_jobs_dirs(self.job_cycle.OutputDirectory, args.workdir)
            dataset.group_files_into_jobs(splitting_mechanism, splitting_value)

            total_jobs += len(dataset.jobs)
            log.info('%s => %d jobs', dataset.name, len(dataset.jobs))

        log.info("TOTAL: %d jobs", total_jobs)
        log.debug(self.datasets)

    def write_batch_files(self, template_root):
        """Make all Datasets write all files necessary for batch jobs.

        This includes both HTCondor job files, and SFrame XML files.
        The template is for XMl files.

        Parameters
        ----------
        template_root : ElementTree.Element
            JobConfiguration element, used as a template for XML files

        """
        for dataset in self.datasets:
            dataset.write_condor_files()
            dataset.write_xml_files(template_root)

    def submit_jobs(self):
        """Submit all jobs across all Datasets, and store log files and status JSON."""
        log.info('Submitting jobs')
        for dataset in self.datasets:
            log.info(dataset.name)
            dataset.submit_jobs()
            dataset.find_job_logs()
            dataset.write_json_status()

    def load_dataset_from_json(self, status_json):
        """Create Dataset and its Jobs from JSON

        TODO: proper Encoder/DEcoders

        Parameters
        ----------
        status_json : str
            JSON status filename
        """
        log.debug("Loading JSON from %s", status_json)
        with open(status_json) as f:
            sdict = json.load(f)

        dataset = Dataset()
        dataset.__dict__.update(sdict)
        # Do jobs manually
        dataset.jobs = [Job(**jdict) for jdict in sdict['jobs']]
        log.debug(dataset)
        self.datasets.append(dataset)
