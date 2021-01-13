#!/usr/bin/env python
# -*- coding: utf-8 -*-

from io_func import *
from batch_classes import *
from Inf_Classes import *
from SubmissionInfo_Class import *

import os
import subprocess
import itertools
import datetime
import json
import time
import gc
import shutil

import StringIO
from xml.dom.minidom import parse, parseString
import xml.sax


# takes care of looking into qstat 
class pidWatcher(object):
    def __init__(self,subInfo):
        self.pidStates = {}
        ListOfPids = [subInfo[k].arrayPid for k in range(len(subInfo)) if len(subInfo[k].arrayPid) > 0 ]
        # adding Pids of resubmitted jobs
        for process in subInfo:
            for pid in process.pids:
                if process.arrayPid not in pid:
                    ListOfPids.append(pid)
        if(len(ListOfPids)==0):
            self.parserWorked = False
            return
        try:
            #looking into condor_q for jobs that are idle, running or hold (HTC State 1,2 and 5)
            proc_cQueue = subprocess.Popen(['condor_q']+ListOfPids+['-af:,','GlobalJobId','JobStatus'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            cQueue_jsons = proc_cQueue.communicate()[0]
            processes_json = []
            if cQueue_jsons:
                for line in cQueue_jsons.split('\n'):
                    if(',' not in line):
                        continue
                    GlobalJobId, JobStatus = line.strip().split(',')
                    processes_json.append({'GlobalJobId':GlobalJobId,'JobStatus':int(JobStatus)})
                self.parserWorked = True
            else:
                self.parserWorked = False
        except Exception as e:
            # print e
            self.parserWorked = False
            print 'Processing condor_q information did not work. Maybe the NAF has some problem. Or nothing is running on the Batch anymore.'
            print 'Going to wait for 5 minutes, lets see if condor_q will start to work again.'
            time.sleep(300)
            return
        if(any(len(i)==0 for i in ListOfPids)):
            self.parserWorked=False
        if self.parserWorked:
            for item in processes_json:
                raw_id = item["GlobalJobId"]
                jobid = raw_id.split("#")[1]
                if(jobid.split('.')[0] not in ListOfPids):
                    continue
                self.pidStates.update({jobid:item["JobStatus"]})

    def check_pidstatus(self, pid, debug=False):
        if '.' not in str(pid): pid = pid + '.0' 
        if pid not in self.pidStates:
            return -1
        state = str(self.pidStates[pid])
        if(state == '1' or state == '2' or state == '4'):
            return 1  # in the batch
        else:
            return 2  # error state
        return 0  # not available

#JSON Format is used to store the submission information
class HelpJSON:
    def __init__(self,json_file):
        self.data = None
        #return
        if os.path.isfile(json_file):
            print 'Using saved settings from:', json_file
            self.data = json.load(open(json_file,'r'))
            #self.data = json.load(self.data)

    def check(self,datasetname):
        for element in self.data:
            jdict = json.loads(element)
            if str(datasetname) == str(jdict['name']) and (str(jdict['arrayPid']) or any(jdict['pids'])):
                print 'Found Submission Info for',jdict['name']
                mysub = SubInfo()
                mysub.load_Dict(jdict)
                return mysub
        return None

class JobManager(object):
    def __init__(self,options,header,workdir):
        self.header = header #how do I split stuff, sframe_batch header in xml file
        self.workdir = workdir #name of the workdir
        self.merge  = MergeManager(options.add,options.forceMerge,options.waitMerge,options.addNoTree)
        self.subInfo = [] #information about the submission status
        self.deadJobs = 0 #check if no file has been written to disk and nothing is on running on the batch
        self.totalFiles = 0  
        self.missingFiles = -1
        self.move_cursor_up_cmd = None # pretty print status
        self.stayAlive = 0 # loop counter to see if program is running 
        self.numOfResubmit =0
        self.watch = None
        self.printString = []
        self.keepGoing = options.keepGoing
        self.exitOnQuestion = options.exitOnQuestion
        self.outputstream = self.workdir+'/Stream_'
        self.el7_worker = options.el7worker  # enforce running on EL7 mahcine
        self.local = options.local  # enforce running interactively
        self.use_grid_control = options.gc  # do resubmissions locally via grid-control instead of BIRD

    #read xml file and do the magic 
    def process_jobs(self,InputData,Job):
        jsonhelper = HelpJSON(self.workdir+'/SubmissinInfoSave.p')
        number_of_processes = len(InputData)
        gc.disable()
        for process in xrange(number_of_processes):
            found = None
            processName = ([InputData[process].Version])
            if jsonhelper.data:
                helpSubInfo = SubInfo()
                found = jsonhelper.check(InputData[process].Version)
                if found:
                    self.subInfo.append(found)
            if not found:
                self.subInfo.append(SubInfo(InputData[process].Version,write_all_xml(self.workdir+'/'+InputData[process].Version,processName,self.header,Job,self.workdir),InputData[process].Type))
            if self.subInfo[-1].numberOfFiles == 0:
                print 'Removing',self.subInfo[-1].name
                self.subInfo.pop()
            else:
                self.totalFiles += self.subInfo[-1].numberOfFiles
                self.subInfo[-1].reset_resubmit(self.header.AutoResubmit) #Reset the retries every time you start
                write_script(processName[0],self.workdir,self.header,self.el7_worker) #Write the scripts you need to start the submission
        gc.enable()
    #submit the jobs to the batch as array job
    #the used function should soon return the pid of the job for killing and knowing if something failed
    def submit_jobs(self,OutputDirectory,nameOfCycle):
        for process in self.subInfo:
            process.startingTime = time.time()
            process.arrayPid = submit_qsub(process.numberOfFiles,self.outputstream+str(process.name),str(process.name),self.workdir,local=self.local)
            print 'Submitted jobs',process.name, 'pid', process.arrayPid
            process.reachedBatch = [False]*process.numberOfFiles
            if process.status != 0:
                process.status = 0
            process.pids=[process.arrayPid+'.'+str(i) for i in range(process.numberOfFiles)]
            # if any(process.pids): 
            #     process.pids = ['']*process.numberOfFiles
    #resubmit the jobs see above      
    def resubmit_jobs(self):
        qstat_out = self.watch.parserWorked
        ask = True

        # first count the number to see if sensible to run each individually
        num_to_resubmit = 0
        for process in self.subInfo:
            for it in process.missingFiles:
                batchstatus = self.watch.check_pidstatus(process.pids[it-1])
                if batchstatus != 1:
                    num_to_resubmit += 1
        # print("numOfResubmit:", self.numOfResubmit, "num_to_resubmit:", num_to_resubmit)
        max_local_resubmit = 6
        if self.local and num_to_resubmit > max_local_resubmit and not self.use_grid_control:
            raise RuntimeError("Do not resubmit %d jobs locally! Maximum of %d allowed, or use --gc" % (num_to_resubmit, max_local_resubmit))

        print "num_to_resubmit", num_to_resubmit
        frac_resubmit_check = 0.5
        if num_to_resubmit > (frac_resubmit_check * self.totalFiles):
            if self.exitOnQuestion:
                exit(-1)
            elif not self.keepGoing:
                res = raw_input('Over %g%% of jobs (%g%%) need resubmitting. Do you really want to resubmit? Y/[N]' % (100*frac_resubmit_check, 100 * num_to_resubmit / self.totalFiles))
                if res.lower() != 'y':
                    exit(-1)

        list_of_xml_to_resub = []

        for process in self.subInfo:
            for it in process.missingFiles:
                batchstatus = self.watch.check_pidstatus(process.pids[it-1])
                if qstat_out and batchstatus==-1 and ask:
                    print '\n' + str(qstat_out)
                    if self.exitOnQuestion:
                        exit(-1)
                    elif not self.keepGoing:
                        res = raw_input('Some jobs are still running (see above). Do you really want to resubmit? Y/[N] ')
                        if res.lower() != 'y':
                            exit(-1)
                    ask = False
                if batchstatus != 1:
                    if not self.use_grid_control:
                        process.pids[it-1] = resubmit(Stream=self.outputstream+process.name,
                                                      name=process.name+'_'+str(it),
                                                      workdir=self.workdir,
                                                      header=self.header,
                                                      el7_worker=self.el7_worker,
                                                      local=self.local)
                        #print 'Resubmitted job',process.name,it, 'pid', process.pids[it-1]
                        self.printString.append('Resubmitted job '+process.name+' '+str(it)+' pid '+str(process.pids[it-1]))
                    else:
                        list_of_xml_to_resub.append(os.path.abspath(os.path.join(self.workdir, process.name+'_'+str(it)+".xml")))
                        process.pids[it-1] = "0" # FIXME

                    if process.status != 0: process.status =0
                    process.reachedBatch[it-1] = False

        if self.use_grid_control:
            print "Running via grid-control"
            conf_basename = "sframe_leftover_%s.conf" % (str(process.name).replace(" ", "_"))
            conf_name = os.path.join(self.workdir, conf_basename)
            workdir = os.path.join(self.workdir, 'work.%s' % os.path.splitext(conf_basename)[0])
            if os.path.isdir(workdir):
                print "Deleting old workdir"
                shutil.rmtree(workdir)
            print "Writing new conf:", conf_name
            with open(conf_name, 'w') as f:
                f.write("[global]\n")
                f.write('workdir create = True\n')
                if self.local:
                    f.write('backend = host\n')  # run locally
                else:
                    f.write('backend = local\n')  # Send to local batch system

                f.write('[workflow]\n')
                f.write('task = UserTask\n')

                f.write('[jobs]\n')
                f.write('wall time = 3:00\n')  # in hours
                f.write('max retry = 2\n')
                f.write('memory = 2000\n')  # in MB
                if self.local:
                    f.write('in flight = 6\n')

                f.write('[backend]\n')
                f.write('wait idle = 60\n')  # in seconds

                f.write("[UserTask]\n")
                f.write('executable = sframe_gc.sh\n')
                # need to transfer env vars ourselves
                f.write("constants = LD_LIBRARY_PATH_STORED PATH_STORED CMSSW_BASE SFRAME_DIR\n")
                f.write('LD_LIBRARY_PATH_STORED = %s\n' % os.getenv('LD_LIBRARY_PATH'))
                f.write('PATH_STORED = %s\n' % os.getenv('PATH'))
                f.write('CMSSW_BASE = %s\n' % os.getenv('CMSSW_BASE'))
                f.write('SFRAME_DIR = %s\n' % os.getenv('SFRAME_DIR'))
                # add XML filenames as arguments
                f.write("parameters = FILENAME\n")
                arg_str = " ".join(list_of_xml_to_resub)
                f.write("FILENAME = %s\n" % arg_str)

            print "Please submit with:"
            print "    grid-control", conf_name

    #see how many jobs finished, were copied to workdir
    def check_jobstatus(self, OutputDirectory, nameOfCycle,remove = False, autoresubmit = True):
        missing = open(self.workdir+'/missing_files.txt','w+')
        waitingFlag_autoresub = False
        missingRootFiles = 0 
        ListOfDict =[]
        self.watch = pidWatcher(self.subInfo)
        ask = True
        for i in xrange(len(self.subInfo)-1, -1, -1):
            process = self.subInfo[i]
            ListOfDict.append(process.to_JSON())
            rootFiles =0
            self.subInfo[i].missingFiles = []
            for it in range(process.numberOfFiles):
                if process.jobsDone[it]: 
                    rootFiles+=1
                    continue
                #have a look at the pids with qstat
                batchstatus = self.watch.check_pidstatus(process.pids[it])
                #kill batchjobs with error otherwise update batchinfo
                batchstatus = process.process_batchStatus(batchstatus,it)
                #check if files have arrived 
                filename = OutputDirectory+'/'+self.workdir+'/'+nameOfCycle+'.'+process.data_type+'.'+process.name+'_'+str(it)+'.root'
                #if process.jobsRunning[it]:
                #print filename, os.path.exists(filename), process.jobsRunning[it], process.jobsDone[it], process.arrayPid, process.pids[it]
                # delete files that are far too small to be a real ROOT file (typically 63 Bytes, but we choose something tiny)
                # getsize() returns in bytes
                if os.path.exists(filename) and os.path.getsize(filename) < 1000:
                    print filename, "has size", os.path.getsize(filename), "bytes - deleting"
                    os.remove(filename)
                if os.path.exists(filename) and process.startingTime < os.path.getctime(filename) and not process.jobsRunning[it]:
                    process.jobsDone[it] = True
                if not process.jobsDone[it]:
                    missing.write(self.workdir+'/'+nameOfCycle+'.'+process.data_type+'.'+process.name+'_'+str(it)+'.root  sframe_main '+process.name+'_'+str(it+1)+'.xml\n')
                    self.subInfo[i].missingFiles.append(it+1)
                    missingRootFiles +=1
                else:
                    rootFiles+=1
                #auto resubmit if job dies, take care that there was some job before and warn the user if more then 10% of jobs die 
                #print process.name,'batch status',batchstatus, 'process.reachedBatch',process.reachedBatch, 'process status',process.status,'resubmit counter',process.resubmit[it], 'resubmit active',autoresubmit
                if (
                    process.notFoundCounter[it] > 5 and
                    not process.jobsRunning[it] and
                    not process.jobsDone[it] and 
                    process.reachedBatch[it] and
                    (process.resubmit[it] ==-1 or process.resubmit[it]>0) and
                    (process.pids[it] or process.arrayPid) and
                    autoresubmit
                ):
                    if float(self.numOfResubmit)/float(self.totalFiles) >.10 and ask:
                        if self.exitOnQuestion:
                            exit(-1)
                        elif not self.keepGoing:
                            res = raw_input('More then 10% of jobs are dead, do you really want to continue? Y/[N] ')
                            if res.lower() != 'y':
                                exit(-1)
                        ask = False
                    #print 'resubmitting', process.name+'_'+str(it+1),es not Found',process.notFoundCounter[it], 'pid', process.pids[it], process.arrayPid, 'task',it+1
                    waitingFlag_autoresub = True
                    process.pids[it] = resubmit(self.outputstream+process.name,process.name+'_'+str(it+1),self.workdir,self.header,self.el7_worker)
                    process.status = 0
                    #print 'AutoResubmitted job',process.name,it, 'pid', process.pids[it]
                    self.printString.append('File Found '+str(os.path.exists(filename)))
                    if os.path.exists(filename): self.printString.append('Timestamp is ok '+str(process.startingTime < os.path.getctime(filename)))
                    self.printString.append('AutoResubmitted job '+process.name+' '+str(it)+' pid '+str(process.pids[it]))
                    #time.sleep(5)
                    process.reachedBatch[it] = False
                    if process.resubmit[it] > 0 : 
                        process.resubmit[it] -= 1
                        self.numOfResubmit +=1
            # final status updates
            if (
                any( i > 6 for i in process.notFoundCounter) and
                not any(process.jobsRunning) and
                not all(process.jobsDone) and
                all(process.reachedBatch) # basically set to error when nothing is running anymore & everything was on the batch
            ):
                process.status = 4
            ###Debugging is ongoing
            """
            if any( i > 6 for i in process.notFoundCounter):
                print 'Process', process.name,'not found i-times',i
                print 'Jobs Running? ', any(process.jobsRunning)
                print 'Jobs Done?', all(process.jobsDone)
                print 'Jobs reached Batch?', all(process.reachedBatch)
            """
            if all(process.jobsDone) and not process.status == 2:
                process.status = 1
            process.rootFileCounter=rootFiles
        try:
            missing.close()
        except IOError as e:
            print "I/O error({0}): {1}".format(e.errno, e.strerror)
            
        self.missingFiles = missingRootFiles
        #Save/update pids and other information to json file, such that it can be loaded and used later
        try:
            jsonFile = open(self.workdir+'/SubmissinInfoSave.p','wb+')
            json.dump(ListOfDict, jsonFile)
            jsonFile.close()
        except IOError as e:
            print "I/O error({0}): {1}".format(e.errno, e.strerror)
        if(waitingFlag_autoresub): time.sleep(5)
        
                
    #print status of jobs 
    def print_status(self):
        if not self.move_cursor_up_cmd:
            self.move_cursor_up_cmd = '\x1b[1A\x1b[2K'*(len(self.subInfo) + 3)
            self.move_cursor_up_cmd += '\x1b[1A' # move once more up since 'print' finishes the line
            print 'Status of files'
        else:
              print self.move_cursor_up_cmd
              #time.sleep(.1)  # 'blink'
        
        for item in self.printString:
            print item
        self.printString = []

        stayAliveArray = ['|','/','-','\\']
        if self.stayAlive < 3:
           self.stayAlive +=1  
        else:
            self.stayAlive = 0
        process_names_length = str(len(max([process.name for process in self.subInfo], key=len)))
        print ('%'+process_names_length+'s: %6s %6s %.6s')% ('Sample Name','Ready','#Files','[%]')
        readyFiles =0

        for process in self.subInfo:
            status_message = ['\033[94m Working \033[0m','\033[92m Transferred \033[0m','Merging','Already Merged','\033[91m Failed \033[0m']
            #print process.status
            print ('%'+process_names_length+'s: %6i %6i %.3i')% (process.name, process.rootFileCounter,process.numberOfFiles, 100*float(process.rootFileCounter)/float(process.numberOfFiles)), status_message[process.status]
            readyFiles += process.rootFileCounter
        print 'Number of files: ',readyFiles,'/',self.totalFiles,'(%.3i)' % (100*(1-float(readyFiles)/float(self.totalFiles))),stayAliveArray[self.stayAlive],stayAliveArray[self.stayAlive]
        print '='*80
    
    #take care of merging
    def merge_files(self,OutputDirectory,nameOfCycle,InputData):
        self.merge.merge(OutputDirectory,nameOfCycle,self.subInfo,self.workdir,InputData,self.outputstream)
    #wait for every process to finish
    def merge_wait(self):
        self.merge.wait_till_finished()
    #see how many jobs finished (or error)
    def get_subInfoFinish(self):
        for process in self.subInfo:
            if process.status==0:
                return False
        return True

#class to take care of merging (maybe rethink design)
class MergeManager(object):
    def __init__(self,add,force,wait,onlyhist=False):
        self.add = add
        self.force = force
        self.active_process=[]
        self.wait = wait
        self.onlyhist = onlyhist

    def get_mergerStatus(self):
        if self.add or self.force or self.onlyhist:
            return True
        else:
            return False

    def merge(self,OutputDirectory,nameOfCycle,info,workdir,InputData,outputdir):
        if not self.add and not self.force and not self.onlyhist: return  
        #print "Don't worry your are using nice = 10" 
        OutputTreeName = ""
        for inputObj in InputData:
            for mylist in inputObj.io_list.other:
                if "OutputTree" in mylist:
                    OutputTreeName= mylist[2]
        for process in info:
            if not process.numberOfFiles == process.rootFileCounter:
                continue
            #print any(process.jobsRunning)
            #print process.name,any(process.jobsRunning), process.status ==1,os.path.exists(OutputDirectory+'/'+nameOfCycle+'.'+process.data_type+'.'+process.name+'.root'
            if (not os.path.exists(OutputDirectory+'/'+nameOfCycle+'.'+process.data_type+'.'+process.name+'.root') and all(process.jobsDone) and process.status !=2 ) or self.force:
                self.active_process.append(add_histos(OutputDirectory,nameOfCycle+'.'+process.data_type+'.'+process.name,process.numberOfFiles,workdir,OutputTreeName,self.onlyhist,outputdir+process.name))
                process.status = 2
            #elif process.status !=2: 
            #    process.status = 3

    def wait_till_finished(self):
        if not self.wait: return
        for process in self.active_process:
            if not process: continue
            print 'Active process',process.communicate()[0]
            if not process.poll():
                process.wait()
                #os.kill(process.pid,-9)
