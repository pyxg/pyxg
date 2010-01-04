import time
import threading
import base64
import StringIO 

from XgridFoundation import *
from Foundation import *

#hostname = 'cojo.scu.edu'
#portnumber = 4111


"""PyXgrid is a Python Interface to Xgrid.

Things to do:

1. Try runloop in a different thread again

2. Documentation

3. Authentication variants
    - None
    - Two way random
    - Kerberos

4. Connections by NetService

5. Job output streams and files

6. Resources and KVO's

7. PyObjC issues
    - Construction (init())
    - Memory

"""

def rlwait(nsec):
    """Runs the current NSRunLoop for nsec seconds."""
    runLoop = NSRunLoop.currentRunLoop()
    until = NSDate.dateWithTimeIntervalSinceNow_(nsec)
    runLoop.runUntilDate_(until)

# resource states:

XGResourceStateUninitialized = 0
XGResourceStateOffline = 1
XGResourceStateConnecting = 2
XGResourceStateUnavailable = 3
XGResourceStateAvailable = 4
XGResourceStateWorking = 5
XGResourceStatePending = 6
XGResourceStateStarting = 7
XGResourceStateStagingIn = 8
XGResourceStateRunning = 9
XGResourceStateSuspended = 10
XGResourceStateStagingOut = 11
XGResourceStateCanceled = 12
XGResourceStateFailed = 13
XGResourceStateFinished = 14

_XGResourceStateNames = [
    "Uninitialized",
    "Offline",
    "Connecting",
    "Unavailable",
    "Available",
    "Working",
    "Pending",
    "Starting",
    "StagingIn",
    "Running",
    "Suspended",
    "StagingOut",
    "Canceled",
    "Failed",
    "Finished"
    ]

class Resource(NSObject):
    
    def initWithObject(self, obj):
        self._resource = obj
        self.state()
        self._resource.addObserver_forKeyPath_options_context_( self, "state",
            (NSKeyValueObservingOptionNew | NSKeyValueObservingOptionOld), 0)
        return self    
        
    def identifier(self):
        id = self._resource.identifier()
        rlwait(1.0)
        return id
        
    def state(self):
        self._state = self._resource.state()
        rlwait(1.0)
        return self._state
        
    def stateName(self):
        self.state()
        return _XGResourceStateNames[self._state]

    # KVO Methods

    def removeObservers(self):
        self._resource.removeObserver_forKeyPath_(self,"state")
        
    def observeValueForKeyPath_ofObject_change_context_(self, keyPath, obj, change, context):
        print "KVO in process: ", keyPath, object, change, context
    


class Controller(NSObject):
    """Provides a Pythonic interface to an Xgrid controller.
    
    This class wraps 3 classes in the Cocoa Xgrid API:
    
    1. XGConnnection
    2. XGController
    3. XGAuthenticator
    
    There three classes work together to provide the basic client interface to an Xgrid
    controller.
    """
    
    def connect(self,hostname='',portnumber=4111,password='xgridatscu'):

        # Create a XGConnection instance
        if hostname:
            # Make a connection to the given hostname
            self._connection = XGConnection.alloc().initWithHostname_portnumber_(hostname,portnumber)
        #else if not hostname:
            # Make a connection using Rendevous
        
        # Handle Authentication
        if password:
            self._authenticator = XGTwoWayRandomAuthenticator.alloc().init()
            self._authenticator.setUsername_('one-xgrid-client')
            self._authenticator.setPassword_(password)
            self._connection.setAuthenticator_(self._authenticator)
        else:
            self._authenticator = None
        
        # Make this class the delegate of the XGConnection
        self._connection.setDelegate_(self)
        
        # Attempt open and handle errors
        self._connection.open()
        rlwait(2.0)
        
        # Now create the controller
        self._controller = XGController.alloc().initWithConnection_(self._connection)
        self._controller.addObserver_forKeyPath_options_context_(self, "state", 
            (NSKeyValueObservingOptionNew | NSKeyValueObservingOptionOld), 0)
        rlwait(2.0)
            
    def disconnect(self):
        self._controller.removeObserver_forKeyPath_(self,"state")
        self._connection.close()
        rlwait(2.0)

    def submit(self, jobSpecification, grid=''):
        if not grid:
            grid = self.defaultGrid().identifier()
        action = Action.alloc().initWithObject(
            self._controller.performSubmitJobActionWithJobSpecification_gridIdentifier_(
            jobSpecification.jobspec, grid))
        outcome = action.outcome()
        print "Job submitted with outcome: ", outcome
        if outcome == 1:
            return action.results()
        else:
            return action.error()
    
    def grids(self):
        rawgridlist = self._controller.grids()
        #rlwait(1.0)
        gridlist = []
        for rg in rawgridlist:
          grid = Grid.alloc().initWithObject(rg)
          gridlist.append(grid)
        return gridlist

    def defaultGrid(self):
        rg = self._controller.defaultGrid()
        #rlwait(1.0)
        return Grid.alloc().initWithObject(rg)
    
    # Resource Methods
    
    def identifier(self):
        id = self._controller.identifier()
        rlwait(1.0)
        return id    
        
    def state(self):
        self._state = self._controller.state()
        rlwait(1.0)
        return self._state
        
    def stateName(self):
        self.state()
        return _XGResourceStateNames[self._state]
        
    # Delegate and KVO methods
    
    def connectionDidOpen_(self, connection):
        print "Connection did open: ", connection.name()
        connection.addObserver_forKeyPath_options_context_(self, "state", 
            (NSKeyValueObservingOptionNew | NSKeyValueObservingOptionOld), 0)
        
    def connectionDidNotOpen_withError_(self, connection, error):
        print "Error in connecting: ", connection.name(), error
    
    def connectionDidClose_(self, connection):
        print "Connection did close: ", connection.name()
        connection.removeObserver_forKeyPath_(self,"state")
        
    def observeValueForKeyPath_ofObject_change_context_(self, keyPath, object, change, context):
        print "KVO in process: ", keyPath, object, change, context

class Grid(Resource):
    
    def initWithObject(self,obj):
        self = Resource.initWithObject(self, obj)
        self._grid = obj
        return self
        
    def default(self):
        isdefault = self._grid.isDefault()
        rlwait(1.0)
        return isdefault
        
    def jobs(self):
        rawjoblist = self._grid.jobs()
        rlwait(1.0)
        joblist = []
        for rj in rawjoblist:
          job = Job.alloc().initWithObject(rj)
          joblist.append(job)
        return joblist
        
    def deleteAll(self):
        for job in self.jobs():
            job.delete()
    
    def activeCPUPower(self):
        cpupower = 0.0
        for job in self.jobs():
            cpupower += job.activeCPUPower()
        return cpupower
        
    def taskCount(self):
        tasks = 0
        for job in self.jobs():
            tasks += job.taskCount()
        return tasks
        
    def job(self,id):
        job_obj = self._grid.jobForIdentifier_(id)
        rlwait(1.0)
        job = Job.alloc().initWithObject(job_obj)
        return job
    
class Job(Resource):
    
    def initWithObject(self,obj):
        self = Resource.initWithObject(self, obj)
        self._job = obj
        return self
        
    def stop(self):
        action = Action.alloc().initWithObject(self._job.performStopAction())
        return action.outcome()
 
    def restart(self):
        action = Action.alloc().initWithObject(self._job.performRestartAction())
        return action.outcome()
        
    def suspend(self):
        action = Action.alloc().initWithObject(self._job.performSuspendAction())
        return action.outcome()
        
    def resume(self):
        action = Action.alloc().initWithObject(self._job.performResumeAction())
        return action.outcome()
        
    def delete(self):
        action = Action.alloc().initWithObject(self._job.performDeleteAction())
        return action.outcome()
                    
    def info(self):
        action = Action.alloc().initWithObject(self._job.performGetSpecificationAction())
        outcome = action.outcome()
        if outcome == 1:
            return action.results()
        
    def streams(self):
        action = Action.alloc().initWithObject(self._job.performGetOutputStreamsAction())
        outcome = action.outcome()
        if outcome == 1:
            return action.results()    
                
    def files(self):
        action = Action.alloc().initWithObject(self._job.performGetOutputFilesAction())
        outcome = action.outcome()
        if outcome == 1:
            return action.results()  
              
    def applicationIdentifier(self):
        return self._job.applicationIdentifier()
        
    def applicationInfo(self):
        return self._job.applicationInfo()
        
    def activeCPUPower(self):
        return self._job.activeCPUPower()
    
    def percentDone(self):
        return self._job.percentDone()

    def taskCount(self):
        return self._job.taskCount()
        
    def completedTaskCount(self):
        return self._job.completedTaskCount()
        
    def dateSumbitted(self):
        return self._job.dateSubmitted()
        
    def dateStarted(self):
        return self._job.dateStarted()
        
    def dateStopped(self):
        return self._job.dateStopped()
        
    def status(self):
        status = {}
        status['applicationIdentifier'] = self.applicationIdentifier()
        status['applicationInfo'] = self.applicationInfo()
        status['activeCPUPower'] = self.activeCPUPower()
        status['percentDone'] = self.percentDone()
        status['taskCount'] = self.taskCount()
        status['completedTaskCount'] = self.completedTaskCount()
        status['dateSumbitted'] = self.dateSumbitted()
        status['dateStarted'] = self.dateStarted()
        status['dateStopped'] = self.dateStopped()
        return status

class Action(NSObject):
    
    def initWithObject(self, obj):
        self._action = obj
        self._outcome = self._action.outcome()
        self._results = self._action.results()
        rlwait(1.0)
        return self
        #self.action.addObserver_forKeyPath_options_context_(self, "outcome", 
        #    (NSKeyValueObservingOptionNew | NSKeyValueObservingOptionOld), 0)

    #def observeValueForKeyPath_ofObject_change_context_(self, keyPath, object, change, context):
    #    if keyPath == 'outcome':
    #        print change
            
    def outcome(self):
        self._outcome = self._action.outcome()
        while not self._outcome:
            rlwait(1.0)
            self._outcome = self._action.outcome()
        return self._outcome
       
    def error(self):
        error = self._action.error()
        rlwait(1.0)
        return error
    
    def results(self):
        if not self._outcome: 
            self.outcome()
        if self._outcome == 1:
            self._results = self._action.results()
        elif self._outcome == 2:
            self._results = {}
        return self._results
        
    def actionDidSucceed(self):
        self.outcome()
        if self._outcome == 1:
            return 1
            
    def actionDidFail(self):
        self.outcome()
        if self._outcome == 2:
            return 1
        
class JobSpecification(NSObject):
    
    def init(self):
        self.jobspec = {}
        self.jobspec['taskSpecifications'] = {}
        self.tasks = []
        self.next_task = 0
        self.jobspec['applicationIdentifier'] = "pyxgrid"
        return self
        
    def name(self, name):
        self.jobspec['name'] = name
        
    def email(self, email):
        self.jobspec['notificationEmail'] = email
        
    def checkSchedulerParameters(self):
        if not self.jobspec.has_key('schedulerParameters'):
            self.jobspec['schedulerParameters'] = {}
                            
    def checkInputFiles(self):
        if not self.jobspec.has_key('inputFiles'):
            self.jobspec['inputFiles'] = {}

    def tasksMustStartSimultaneously(self):
        self.checkSchedulerParameters()
        self.jobspec['schedulerParameters']['tasksMustStartSimultaneously'] = 'YES'
        
    def minimumTaskCount(self,count):
        self.checkSchedulerParameters()
        self.jobspec['schedulerParameters']['minimumTaskCount'] = count
    
    def dependsOnJobs(self,jobarray):
        self.checkSchedulerParameters()
        self.jobspec['schedulerParameters']['dependsOnJobs'] = jobarray
        
    def addFile(self, filepath, filename, isExecutable):
        data = NSData.dataWithContentsOfFile_(filepath)
        self.checkInputFiles()
        if isExecutable == 0:
            isExecString = 'NO'
        elif isExecutable == 1:
            isExecString = 'YES'
        else:
            isExecSTring = 'NO'
        self.jobspec['inputFiles'][filename] = {'fileData':data,'isExecutable':isExecString}
    
    def addTask(self, command='', arguments=[], environment={}, inputStream='', dependsOnTasks=[]):
        taskspec = {}
        taskname = 'task%i' % self.next_task
        self.next_task += 1
        taskspec['command'] = command
        if arguments:
            taskspec['arguments'] = arguments
        if environment:
            taskspec['environment'] = environment
        if inputStream:
            taskspec['inputStream'] = inputStream
        if dependsOnTasks:
            taskspec['dependsOnTasks'] = dependsOnTasks
        self.jobspec['taskSpecifications'][taskname] = taskspec
        
    def copyTask(self):
        pass
        
    