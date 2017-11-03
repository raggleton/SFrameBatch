"""Classes to help access parts of the JobConfiguration

Class names should correspond to their XML tags.
"""


from utils import dict_to_str


class JobConfiguration(object):
    """Hold info & objects about the whole configuration"""

    def __init__(self, JobName, OutputLevel, library, package, cycles=None):
        self.JobName = JobName
        self.OutputLevel = OutputLevel
        self.library = library
        self.package = package
        self.cycles = cycles or []

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))


class Cycle(object):
    """Hold info about the uhh AnalysisModuleRunner"""

    def __init__(self, Name, TargetLumi, OutputDirectory="./", PostFix="",
                 ProofServer="", ProofWorkDir="", ProofNodes=-1,
                 UseTreeCache=False, TreeCacheSize=30000000,
                 TreeCacheLearnEntries=100, ProcessOnlyLocal=False,
                 input_datas=None, user_config=None):
        self.Name = Name
        self.TargetLumi = TargetLumi
        self.OutputDirectory = OutputDirectory
        self.PostFix = PostFix
        # Hmm do I want these...double-defining default args as in JobConfig.dtd
        self.ProofServer = ProofServer
        self.ProofWorkDir = ProofWorkDir
        self.ProofNodes = ProofNodes
        self.UseTreeCache = UseTreeCache
        self.TreeCacheSize = TreeCacheSize
        self.TreeCacheLearnEntries = TreeCacheLearnEntries
        self.ProcessOnlyLocal = ProcessOnlyLocal
        self.input_datas = input_datas or []
        self.user_config = user_config

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))



class InputData(object):
    """Hold info about an input dataset"""

    def __init__(self, Type, Version, Lumi=0, NEventsMax=-1, NEventsSkip=0, Cacheable=False, SkipValid=False, input_tree=None, output_tree=None, input_obj=None):
        self.Type = Type
        self.Version = Version
        self.Lumi = Lumi
        self.NEventsMax = NEventsMax
        self.NEventsSkip = NEventsSkip
        self.Cacheable = Cacheable
        self.SkipValid = SkipValid
        self.input_tree = input_tree
        self.output_tree = output_tree
        self.input_obj = input_obj or []

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))


class In(object):
    """Hold single input file info"""

    def __init__(self, FileName, Lumi):
        self.FileName = FileName
        self.Lumi = Lumi

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))


class InputTree(object):
    """Hold info about input tree to read"""

    def __init__(self, Name):
        self.Name = Name

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))


class OutputTree(object):
    """Hold info about output tree to write"""

    def __init__(self, Name):
        self.Name = Name

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))


class UserConfig(object):
    """Settings for the Cycle, not including input files. Holds a collection of Items."""

    def __init__(self, items=None):
        self.items = items or []

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))


class Item(object):
    """Hold info about an <Item>. Many Items make up a UserConfig."""

    def __init__(self, Name, Value):
        self.Name = Name
        self.Value = Value

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

