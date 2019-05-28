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

import StringIO
from xml.dom.minidom import parse, parseString
import xml.sax


# takes care of looking into qstat 
class pidWatcher(object):
    def __init__(self):
        self.pidList = []
        self.taskList = []
        self.stateList = []
        try:
            #with the change from sge to condor this can nwo be a json dict!
            proc_qstat = subprocess.Popen(['condor_q','-json','-attributes','JobStatus,GlobalJobId'],stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = proc_qstat.communicate()
            # This is a really ugly hack to workaround a htcondor bug
            # that outputs a message when you don't include ProcId
            # Fixed in condor version 8.8: https://github.com/htcondor/htcondor/commit/2257b30bbdbb3260b8341403c57c51377455acad#diff-f6a8cce84bc100939e0a50202ffe3602
            # This hides STDERR unless something other than the
            # annoying message occurs
            for line in err.splitlines():
                if "Two results with the same ID" not in line:
                    print err
                    break
            qstat_xml =  StringIO.StringIO(out)
            if qstat_xml.getvalue():
                qstat_xml_par = json.loads(qstat_xml.getvalue())            
                self.parserWorked = True
            else:
                self.parserWorked = False
        except Exception as e:
            #print e
            self.pidTaskList = []
            self.parserWorked = False
            print 'Processing qstat information did not work. Maybe the NAF has some problem. Or nothing is running on the Batch anymore.'
            print 'Going to wait for 5 minutes, lets see if qstat will start to work again.'
            time.sleep(10)
            return 
       
        if self.parserWorked: 
          for item in qstat_xml_par:
            #print item
            raw_id = item.get("GlobalJobId")
            jobid = raw_id.split("#")[1]
            self.pidList.append(jobid)
            self.stateList.append(item.get("JobStatus"))
            
   
    def check_pidstatus(self,arraypid,pidlist,task,debug=False):
        pid = 0
        if pidlist:
            pid = int(pidlist)
        elif arraypid > 0:
            pid = arraypid
        else:
            return -1

        for i in range(len(self.pidList)):
            inrange = False

            if debug and str(self.pidList[i]) == str(pid): print 'pid', pid, 'task', task, 'pidlist', self.pidList[i], 'state list', self.stateList[i], 'task List', self.taskList[i], 'in range',inrange or self.taskList[i]==-1
            if '.' not in str(self.pidList[i]): self.pidList[i]= self.pidList[i]+'.0' 
            if str(self.pidList[i]) == str(pid):
                if str(self.stateList[i]) == '1' or str(self.stateList[i]) =='2' or str(self.stateList[i]) =='4': # 1 idle; 2 running; 4 completed
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
                write_script(processName[0],self.workdir,self.header) #Write the scripts you need to start the submission
        gc.enable()
    #submit the jobs to the batch as array job
    #the used function should soon return the pid of the job for killing and knowing if something failed
    def submit_jobs(self,OutputDirectory,nameOfCycle):
        for process in self.subInfo:
            process.startingTime = time.time()
            process.arrayPid = submit_qsub(process.numberOfFiles,self.outputstream+str(process.name),str(process.name),self.workdir)
            print 'Submitted jobs',process.name, 'pid', process.arrayPid
            process.reachedBatch = [False]*process.numberOfFiles
            if process.status != 0:
                process.status = 0
            if any(process.pids): 
                process.pids = ['']*process.numberOfFiles
    #resubmit the jobs see above      
    def resubmit_jobs(self):
        qstat_out = self.watch.parserWorked
        ask = True
        for process in self.subInfo:
	    for it in process.missingFiles:
                batchstatus = self.watch.check_pidstatus(process.pids[it-1],process.arrayPid,it)
                if qstat_out and batchstatus==-1 and ask:
                    print '\n' + qstat_out
                    if self.exitOnQuestion:
                        exit(-1)
                    elif not self.keepGoing:
                        res = raw_input('Some jobs are still running (see above). Do you really want to resubmit? Y/[N] ')
                        if res.lower() != 'y':
                            exit(-1)
                    ask = False
                if batchstatus != 1:
                    process.pids[it-1] = resubmit(self.outputstream+process.name,process.name+'_'+str(it),self.workdir,self.header)
                    #print 'Resubmitted job',process.name,it, 'pid', process.pids[it-1]
                    self.printString.append('Resubmitted job '+process.name+' '+str(it)+' pid '+str(process.pids[it-1]))
                    if process.status != 0: process.status =0
                    process.reachedBatch[it-1] = False
                    
    #see how many jobs finished, were copied to workdir 
    def check_jobstatus(self, OutputDirectory, nameOfCycle,remove = False, autoresubmit = True):
        missing = open(self.workdir+'/missing_files.txt','w+')
        waitingFlag_autoresub = False
        missingRootFiles = 0 
        ListOfDict =[]
        self.watch = pidWatcher()
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
                batchstatus = self.watch.check_pidstatus(process.pids[it],process.arrayPid,it+1)
                #kill batchjobs with error otherwise update batchinfo
                batchstatus = process.process_batchStatus(batchstatus,it)
                #check if files have arrived 
                filename = OutputDirectory+'/'+self.workdir+'/'+nameOfCycle+'.'+process.data_type+'.'+process.name+'_'+str(it)+'.root'
                #if process.jobsRunning[it]:
                #print filename, os.path.exists(filename), process.jobsRunning[it], process.jobsDone[it], process.arrayPid, process.pids[it]
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
                    process.pids[it] = resubmit(self.outputstream+process.name,process.name+'_'+str(it+1),self.workdir,self.header)
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

        print '%30s: %6s %6s %.6s'% ('Sample Name','Ready','#Files','[%]')
        readyFiles =0

        for process in self.subInfo:
            status_message = ['\033[94m Working \033[0m','\033[92m Transferred \033[0m','Merging','Already Merged','\033[91m Failed \033[0m']
            #print process.status
            print '%30s: %6i %6i %.3i'% (process.name, process.rootFileCounter,process.numberOfFiles, 100*float(process.rootFileCounter)/float(process.numberOfFiles)), status_message[process.status]
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
