#!/usr/bin/env python

from subprocess import call
from subprocess import Popen
from subprocess import PIPE
import os

from tree_checker import *
#from fhadd import fhadd


SINGULARITY_IMG = os.path.expandvars("/nfs/dust/cms/user/$USER/slc6_latest.sif")


def write_script(name,workdir,header,el7_worker=False):
    sframe_wrapper=open(workdir+'/sframe_wrapper.sh','w')

    # For some reason, we have to manually copy across certain environment
    # variables, most notably LD_LIBRARY_PATH, and if running on singularity, PATH
    # Note that we need the existing PATH, otherwise it loses basename, sed, etc
    # We also need to specify SFRAME_TEMP_DIR ourselves; the default on /tmp
    # doesn't work when running within singularity (don't know why),
    # so we need to put it somewhere else.
    # Note that SFrame will make it's own uniquely named subdirectory
    # within SFRAME_TEMP_DIR and will clean it up after the job has ended
    # (set OutputLevel="DEBUG" to see what it does)
    tmp_dir = os.path.abspath(workdir)
    if not os.path.isdir(tmp_dir):
        os.makedirs(tmp_dir)

    sframe_wrapper.write(
        """#!/bin/bash
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_STORED:$LD_LIBRARY_PATH
export PATH=$PATH_STORED:$PATH
export SFRAME_TEMP_DIR="""+tmp_dir+"""
sframe_main $1
        """)
    sframe_wrapper.close()
    os.system('chmod u+x '+workdir+'/sframe_wrapper.sh')
    if (header.Notification == 'as'):
        condor_notification = 'Error'
    elif (header.Notification == 'n'):
        condor_notification = 'Never'
    elif (header.Notification == 'e'):
        condor_notification = 'Complete'
    else:
        condor_notification = ''

    # Note that it automatically figures out which worker node arch you need
    # based on submit node arch.
    # However we don't always want this.
    # We can enforce a EL7 worker, and if necessary load a SL6 singularity image.
    # Or we just take the desired worker node arch from SCRAM_ARCH
    # (such that one can submit SLC6 worker node jobs from a EL7 node)
    worker_str = ""
    if el7_worker:
        worker_str = 'Requirements = ( OpSysAndVer == "CentOS7" )\n'
        if 'slc6' in os.getenv('SCRAM_ARCH'):
            # Run a SLC6 job on EL7 machine using singularity
            if not os.path.isfile(SINGULARITY_IMG):
                print "Please pull the SLC6 image to your NFS:"
                print ""
                print 'SINGULARITY_CACHEDIR="/nfs/dust/cms/user/$USER/singularity" singularity pull', SINGULARITY_IMG, 'docker://cmssw/slc6:latest'
                print ""
                raise RuntimeError("Cannot find image, %s. Do not use one from /afs or /cvmfs." % SINGULARITY_IMG)
            worker_str += '+MySingularityImage="'+SINGULARITY_IMG+'"\n'
    else:
        # Choose worker node arch based on SCRAM_ARCH (not login node arch)
        if 'slc7' in os.getenv('SCRAM_ARCH'):
            worker_str = 'Requirements = ( OpSysAndVer == "CentOS7" )'
        else:
            worker_str = 'Requirements = ( OpSysAndVer == "SL6" )'

    submit_file = open(workdir+'/CondorSubmitfile_'+name+'.submit','w')
    submit_file.write(
        """#HTC Submission File for SFrameBatch
# +MyProject        =  "af-cms"
""" + worker_str + """
universe          = vanilla
# #Running in local mode with 8 cpu slots
# universe          =  local
# request_cpus      =  8
notification      = """+condor_notification+"""
notify_user       = """+header.Mail+"""
initialdir        = """+workdir+"""
output            = $(Stream)/"""+name+""".o$(ClusterId).$(Process)
error             = $(Stream)/"""+name+""".e$(ClusterId).$(Process)
log               = $(Stream)/"""+name+""".$(Cluster).log
#Requesting CPU and DISK Memory - default +RequestRuntime of 3h stays unaltered
RequestMemory     = """+header.RAM+"""G
RequestDisk       = """+header.DISK+"""G
#You need to set up sframe
getenv            = True
environment       = "LD_LIBRARY_PATH_STORED="""+os.environ.get('LD_LIBRARY_PATH')+""" PATH_STORED="""+os.environ.get('PATH')+""""
JobBatchName      = """+name+"""
executable        = """+workdir+"""/sframe_wrapper.sh
MyIndex           = $(Process) + 1
fileindex         = $INT(MyIndex,%d)
arguments         = """+name+"""_$(fileindex).xml
""")
    submit_file.close()

def resub_script(name,workdir,header,el7_worker=False):
    if (header.Notification == 'as'):
        condor_notification = 'Error'
    elif (header.Notification == 'n'):
        condor_notification = 'Never'
    elif (header.Notification == 'e'):
        condor_notification = 'Complete'
    else:
        condor_notification = ''

    worker_str = ""
    if el7_worker:
        worker_str = 'Requirements = ( OpSysAndVer == "CentOS7" )\n'
        if 'slc6' in os.getenv('SCRAM_ARCH'):
            # Run a SLC6 job on EL7 machine using singularity
            if not os.path.isfile(SINGULARITY_IMG):
                print "Please pull the SLC6 image to your NFS:"
                print ""
                print 'SINGULARITY_CACHEDIR="/nfs/dust/cms/user/$USER/singularity" singularity pull', SINGULARITY_IMG, 'docker://cmssw/slc6:latest'
                print ""
                raise RuntimeError("Cannot find image, %s. Do not use one from /afs or /cvmfs." % SINGULARITY_IMG)
            worker_str += '+MySingularityImage="'+SINGULARITY_IMG+'"\n'
    else:
        if 'slc7' in os.getenv('SCRAM_ARCH'):
            worker_str = 'Requirements = ( OpSysAndVer == "CentOS7" )'
        else:
            worker_str = 'Requirements = ( OpSysAndVer == "SL6" )'

    submitfile = open(workdir+'/CondorSubmitfile_'+name+'.submit','w')
    submitfile.write(
"""#HTC Submission File for SFrameBatch
# +MyProject        =  "af-cms"
""" + worker_str + """
universe          = vanilla
# #Running in local mode with 8 cpu slots
# universe          =  local
# request_cpus      =  8
notification      = """+condor_notification+"""
notify_user       = """+header.Mail+"""
initialdir        = """+workdir+"""
output            = $(Stream)/"""+name+""".o$(ClusterId).$(Process)
error             = $(Stream)/"""+name+""".e$(ClusterId).$(Process)
log               = $(Stream)/"""+name+""".$(Cluster).log
#Requesting CPU and DISK Memory
# RequestMemory     = """+header.RAM+"""G
RequestMemory     = 8G
RequestDisk       = """+header.DISK+"""G
+RequestRuntime = """+str(int(4*3600))+"""
#You need to set up sframe
getenv            = True
environment       = "LD_LIBRARY_PATH_STORED="""+os.environ.get('LD_LIBRARY_PATH')+""" PATH_STORED="""+os.environ.get('PATH')+""""
JobBatchName      = """+name+"""
executable        = """+workdir+"""/sframe_wrapper.sh
arguments         = """+name+""".xml
queue
""")
    submitfile.close()

def submit_qsub(NFiles,Stream,name,workdir):
    #print '-t 1-'+str(int(NFiles))
    #call(['ls','-l'], shell=True)

    if not os.path.exists(Stream):
        os.makedirs(Stream)
        print Stream+' has been created'

    #call(['qsub'+' -t 1-'+str(NFiles)+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'], shell=True)
    # proc_qstat = Popen(['condor_qsub'+' -t 1-'+str(NFiles)+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'],shell=True,stdout=PIPE)
    # return (proc_qstat.communicate()[0].split()[2]).split('.')[0]
    proc_qstat = Popen(['condor_submit'+' '+workdir+'/CondorSubmitfile_'+name+'.submit'+' -a "Stream='+Stream.split('/')[1]+'" -a "queue '+str(NFiles)+'"'],shell=True,stdout=PIPE)
    return (proc_qstat.communicate()[0].split()[7]).split('.')[0]


def resubmit(Stream,name,workdir,header,el7_worker):
    #print Stream ,name
    resub_script(name,workdir,header,el7_worker)
    if not os.path.exists(Stream):
        os.makedirs(Stream)
        print Stream+' has been created'
    #call(['qsub'+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'], shell=True)
    # proc_qstat = Popen(['condor_qsub'+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'],shell=True,stdout=PIPE)
    # return proc_qstat.communicate()[0].split()[2]
    proc_qstat = Popen(['condor_submit'+' '+workdir+'/CondorSubmitfile_'+name+'.submit'+' -a "Stream='+Stream.split('/')[1]+'"'],shell=True,stdout=PIPE)
    return (proc_qstat.communicate()[0].split()[7]).split('.')[0]

def add_histos(directory,name,NFiles,workdir,outputTree, onlyhists,outputdir):
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
    FNULL = open(os.devnull, 'w')
    if os.path.exists(directory+name+'.root'):
        call(['rm '+directory+name+'.root'], shell=True)
    string=''
    proc = None
    position = -1
    command_string = 'nice -n 10 hadd ' # -v 1 ' # the -v stopped working in root 6.06/01 now we get a lot of crap
    if onlyhists: command_string += '-T '
    if(outputTree):
        for i in range(NFiles):
            if check_TreeExists(directory+workdir+'/'+name+'_'+str(i)+'.root',outputTree) and position ==-1:
                position = i
                string+=str(i)
                break

    for i in range(NFiles):
        if not position == i and not position == -1:
            string += ','+str(i)
        elif position ==-1:
            string += str(i)
            position = 0

    source_files = ""
    if NFiles > 1:
        source_files = directory+workdir+'/'+name+'_{'+string+'}.root'
    else:
        source_files = directory+workdir+'/'+name+'_'+string+'.root'

    #print command_string+directory+name+'.root '+source_files
    #print outputdir+'/hadd.log'
    if not string.isspace():
        proc = Popen([str(command_string+directory+name+'.root '+source_files+' > '+outputdir+'/hadd.log')], shell=True, stdout=FNULL, stderr=FNULL)
    else:
        print 'Nothing to merge for',name+'.root'
    return proc


