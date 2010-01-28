#****************************************************************************
#  Copyright (C) 2005-2010 Brian Granger <ellisonbg@gmail.com> and
#                          Barry Wark <bwark@u.washington.edu> and
#                          Beat Rupp <beatrupp@gmail.com
#  Distributed under the terms of the BSD License.  
#****************************************************************************

# PyXG-0.3.0

"""PyXG provides a python interface to Apple's Xgrid.  

Xgrid is Apple's software for building and managing clusters of 
Macintosh computers for use in high performance computation.  
See http://www.apple.com/server/macosx/technology/xgrid.html for more details.

This module wraps the xgrid command line in Mac OS X. It will not work with
the Technonogy Previews of Xgrid. The command line is wrapped in this module 
as the goal is to provide an interface to Xgrid that can be used from an
interactive python prompt. The Cocoa API for Xgrid (XgridFoundation) is 
based on an event-loop paradigm and is less well suited for interactive work.
If you want to use Xgrid and python from within a Cocoa application, you should
use XgridFoundation and PyObjC.

Features
========

    1.  Use Xgrid from within python scripts as well as in interactive python
        sessions.
    2.  Create, submit and manage simple (one task) and batch (many task) Xgrid
        jobs using python's elegant syntax.
    3.  Work with multiple Xgrid controllers simultaneouly.
    4.  List available grids for each controller and query their status.

Quick Start
===========

Import xg, create a Connection and Controller object:

>>> from xg import *
>>> conn = Connection(hostname='xgrid.work.com',password='secret')
>>> cont = Controller(conn)

List the grids managed by the controller:

>>> cont.gridIDs()
(0, 3)
>>> cont.grids()
[<Grid with gridID = 0>, <Grid with gridID = 3>]

Work with the default grid, listing active jobs:

>>> g = cont.grid(0)
>>> g.jobIDs()
(229, 230, 231, 232)
>>> g.printJobs()
##################################################
id   Date Started             Status     CPU Power 
##################################################
229  2005-12-22 11:18:47 -0800 Finished   0         
230  2005-12-22 11:18:50 -0800 Finished   0         
231  2005-12-22 11:18:52 -0800 Finished   0         
232  2005-12-22 11:18:55 -0800 Finished   0         

Get a job from the default grid and work with it:

>>> j = g.job(229)      
>>> j.printInfo()
{
    name:  /usr/bin/cal
    jobStatus:  Finished
    taskCount:  1
    undoneTaskCount:  0
    percentDone:  100
}
>>> j.printInfo(verbose=False)
229  2005-12-22 11:18:47 -0800 Finished   0   
>>> j.printSpecification()
{
    applicationIdentifier :  com.apple.xgrid.cli
    taskSpecifications :  {0 = {arguments = (); command = "/usr/bin/cal"; }; }
    name :  /usr/bin/cal
    inputFiles :  {}
    submissionIdentifier :  abc
}

Get job results:

>>> j.results(stdout="job229.out",stderr="job229.err")
Job stdout saved in file: job229.out 

Use a Grid object to submit a single task job:

>>> j = g.submit(cmd='/usr/bin/cal')
Job submitted with id:  234
>>> j.printInfo(verbose=False)
234  2005-12-22 13:09:52 -0800 Finished   0      
"""

#####################################################################
# Imports                                                           #
##################################################################### 

#import string
#import os, sys
import time
import commands
import re
import itertools
import tempfile
import os.path
from functools import wraps

try:
    import Foundation
    import objc
except ImportError, e:
    print "This module requires PyObjC."
    raise e
    
#####################################################################
# Exceptions                                                        #
#####################################################################  

class XgridError(Exception):
    """Xgrid exception class."""
    def __init__(self, err):
        self.err = err
        
    def __repr__(self):
        return "Xgrid Error: %s" % (self.err)
    
class InvalidIdentifier(XgridError):
    """Xgrid exception for invalid job or grid identifiers."""
    def __init__(self, id):
        self.id = id
    def __repr__(self):
        return "Invalid Xgrid Identifier: " + str(self.id)

class InvalidGridIdentifier(InvalidIdentifier):
    """Invalid grid identifier exception."""
    def __repr__(self):
        return "Invalid Grid Identifier: " + str(self.id)
        
class InvalidJobIdentifier(InvalidIdentifier):
    """Invalid job identifier exception."""
    def __repr__(self):
        return "Invalid Job Identifier: " + str(self.id)
        
class InvalidAction(XgridError):
    """Invalid action exception."""
    def __init__(self, action):
        self.action = action
    def __repr__(self):
        return "Invalid Xgrid Action: " + str(self.action)

class InvalidIdentifierType(Exception):
    """Invalid job or grid identifier type."""
    def __init__(self, bad_var):
        self.bad_var = bad_var
    def __repr__(self):
        return "Invalid Xgrid Identifier Type: " + str(self.bad_var)

# Setting this flag causes printing of every Xgrid command that is executed
PYXGRID_DEBUG = False
VERSION = '0.3.0'

#####################################################################
# See if there is an Xgrid cluster defined by environment vars      #
#####################################################################  

defaultXgridHostname = os.environ.get('XGRID_CONTROLLER_HOSTNAME')
defaultXgridPassword = os.environ.get('XGRID_CONTROLLER_PASSWORD')

if not defaultXgridPassword:
    defaultXgridPassword = ''

#####################################################################
# Utilities for running and parsing Xgrid commands                  #
##################################################################### 

def autorelease(func):
    """A decorator to properly release ObjC object instances.
    
    Anytime an ObjC object instance is created, an NSAutoReleasePool needs to
    be available. PyObjC will create one but it won't get drained very often.
    
    @param func: The function to be decorated.
    """
    
    @wraps(func)
    def wrapped(*args, **kw):
        pool = Foundation.NSAutoreleasePool.alloc().init()
        try:
            func(*args, **kw)
        finally:
            pool.drain()
        del pool

    return wrapped 
    
class NSString(objc.Category(Foundation.NSString)):
    def xGridPropertyList(self):
        """Category to extend NSString.
        
        This enables the handling of illegal 'old-style' plists returned by 
        the xgrid command-line tool.
        
        In particular, on systems before Mac OS X Snow Leopard (10.6) xgrid
        returns "old-style" plists that contain dates that aren't quoted
        strings.  Because old-style plists can't contain dates in native
        format (only as quoted strings), the built-in CoreFoundation 
        parser chokes on the output.
        
        xGridPropertyList: uses a compiled RegEx to add quotes around date
        strings in the xgrid output before passing the result to NSString's
        propertyList:
        """

        str = unicode(self)
        m = re.compile(r'(?P<prefix>^\s*date.* = )(?P<date>.*?);')
        lines = str.splitlines()
        
        for (i, l) in itertools.izip(itertools.count(), lines):
            if (m.search(l)):
                lines[i] = m.sub(r'\g<prefix>"\g<date>";', l)
        
        sep = '\n'
        str = sep.join(lines)
        
        return NSString.stringWithString_(str).propertyList()

def xgridParse(cmd="xgrid -grid list"):
    """Submits and parses output from the xgrid command line.  
    
    The output of the xgrid CLI is a (sometimes illegal) old-style plist.
    This function runs an xgrid command and parses the output of the command
    into a valid NSDictionary (a python dict).
  
    To handle the illegal plists returned by the xgrid CLI, we use the
    xGridPropertyList: method of NSString (defined above).

    See the xgrid man pages for details on the xgrid command.
    This fuction will return a nested python structure that
    reflects the output of the xgrid command.
    """

    # When set, print the actual commands Xgrid sent
    if PYXGRID_DEBUG == True:
        print cmd
        
    # Run the xgrid command
    result = commands.getstatusoutput(cmd)

    # Check for good exit status (0) and parse output
    if result[0] == 0:
        if result[1]:
            return NSString.stringWithString_(result[1]).xGridPropertyList()
        else:
            return {}
    else:
        raise XgridError("xgrid command error: %s" % result[0])

#####################################################################
# Other Utilities                                                   #
#####################################################################  
  
def processID(id):
    """Makes sure that the id is a unicode string"""
    
    if (isinstance(id, str) or isinstance(id, unicode)):
        return unicode(id)
    elif isinstance(id, int):
        return unicode(id)
    else:
        raise InvalidIdentifierType(id)
        
#####################################################################
# Classes                                                           #
#####################################################################

class Connection:
    """Track information needed to connect to an XGrid controller."""
    
    def __init__(self, hostname=0, password=0, kerberos=False):
        """Create a Connection object to be passed to other objects.
        
        To connect to a specific Xgrid controller, create a Connection
        object and then pass it to the Controller, Grid or Job objects
        you create.  This class performs no verification of the hostname 
        or password.  
        
        Examples
        ========
        
        Use the controller and password given in environmental vars.
            
        >>> cn = Connection()
            
        Specify a hostname and password.
            
        >>> cn = Connection('xgrid.work.com','topsecret')
            
        Use Kerberos.
            
        >>> cn = Connection('xgrid.work.com',kerberos=True)

        Usage
        =====
        
        @param hostname: The hostname of the xgrid controller, like
            "xgrid.work.com".  If set to 0, it will default to the value set 
            in the environment variable XGRID_CONTROLLER_HOSTNAME
        @type  hostname: string
        @param password: The password of the xgrid controller, like 
            "mysecret".  If set to 0, it will default to the value set in the
            environment variable XGRID_CONTROLLER_PASSWORD.  For no password,
            set it equal to the empty string: password=''.
        @type  password: string
        @param kerberos: If True, connect using single sign on (SSO), instead 
            of a password.  You must have already obtained a kerberos 
            ticket-granting ticket from the KDC that controlls the kerberos
            domain containing the Xgrid controller. If kerberos is True, the
            password is ignored.
        @type  kerberos: boolean
        """
        
        # Setup the hostname and password
        if hostname == 0:
            if defaultXgridHostname:
                self.hostname = defaultXgridHostname
            else:
                raise XgridError('No controller hostname specified')
        else:
            self.hostname = hostname
        
        if kerberos: # kerberos overrides password
            self.kerberos = True
            self.password = False
        else:
            self.kerberos = False  
            if password == 0:
                self.password = defaultXgridPassword
            else:
                self.password = password
            
        self._buildConnectString()
        
    def _buildConnectString(self):
        """Builds the connect_string."""
        self._connectString = '-h %s ' % self.hostname
        if (self.kerberos):
            self._connectString = '%s-auth Kerberos ' % self._connectString
        else:
            if self.password:
                self._connectString = '%s-p %s ' % \
                    (self._connectString, self.password)
                    
    def connectString(self):
        """Returns the connection string to be used in Xgrid commands."""
        return self._connectString
            
class JobManager:
    """Manage a set of Xgrid jobs."""
    
    def __init__(self, gridID=u'0', connection=None, update=0):
        """Create a JobManager for a given Grid and Connection.
        
        This class is mainly designed to be a base class of the Conroller
        and Grid classes, both of which need to manage Xgrid jobs.  The class
        provides basic capabilities to list active jobs and perform various
        actions on those jobs (stop, restart, resume, suspend, delete).  Job
        submission is handled by the Controller, Grid and Job classes.
        
        Usage
        =====
        
        @arg gridID: The grid identifier of the grid on which the JobManager
            will manage jobs.  Internally, the grid identifier is a unicode
            string, but gridID can be given in any of the formats u'0', '0' 
            or 0.  If gridID=u'0', the JobManager will manage jobs on the
            default grid
        @type gridID: unicode, str or int
        @arg connection:  Instance of Connection class.  If empty a default
            Connection object is used.
        @type connection:  Connection
        @arg update:  A boolean flag that determines whether or not the 
            internal state is updated upon creation.  This involves a call to 
            the Xgrid controller.
        @type update: boolean
        """
        
        self.gridID = processID(gridID)

        if connection is None:
            self._connection = Connection()
        else:    
            self._connection = connection

        self._jobs = []
        self._jobIDs = ()
        if update:
            self._updateJobs()
            
    def _updateJobs(self):
        """Updates the _jobIDs and _jobs instance variables."""
        
        gridIDString = u''
        if self.gridID:
            gridIDString = u'-gid ' + self.gridID 
            
        cmd = 'xgrid %s-job list %s' % (self._connection.connectString(),
            gridIDString)
        result = xgridParse(cmd)
        self._checkGridID(result, self.gridID)
        self._jobIDs = result['jobList']
        
        # Now build the array of Job objects
        self._jobs = []
        for jid in self._jobIDs:
            self._jobs.append(Job(jid, self._connection))

    def _checkGridID(self, result, gridID):
        """Checks a dictionary for an InvalidGridIdentifier error."""
        if result.has_key('error'):
            if result['error'] == 'InvalidGridIdentifier':
                raise InvalidGridIdentifier(gridID)

    def jobs(self, update=1):
        """Returns a list of initialized Job objects for all active jobs.

        @arg update:  A boolean flag that determines whether or not the 
            internal state is updated upon creation.  This involves a call to 
            the Xgrid controller.
        @type update: boolean
        @return: a lists of active Job objects.
        @rtype: list
        """

        if update:
            self._updateJobs()
        return self._jobs

    def job(self, jobID=u'999999999', update=1):
        """Returns the Job object with job identifier id.

        @arg jobID: The job identifier.  Can be given as unicode, str or int.
        @type jobID: unicode, str, or int
        @arg update:  A boolean flag that determines whether or not the 
            internal state is updated upon creation.  This involves a call to 
            the Xgrid controller.
        @type update: boolean
        @return: Initialize Job object.
        """
        
        processedID = processID(jobID)
        
        if update:
            self._updateJobs()
        if processedID in self._jobIDs:
            return Job(processedID, self._connection)
        else:
            raise InvalidJobIdentifier(processedID)

    def jobIDs(self, update=1):
        """Returns a tuple of job identifiers for all active jobs.
        
        @arg update:  A boolean flag that determines whether or not the 
            internal state is updated upon creation.  This involves a call to 
            the Xgrid controller.
        @type update: boolean
        @returns: Tuple of job identifiers.
        @rtype: tuple
        """
        
        if update:
            self._updateJobs()
        return self._jobIDs
    
    # Job management methods
    
    def perform(self, action, jobIDs):
        """Performs an action on a subset of active jobs.
        
        @arg action:  The action to be performed as a string.  Implemented
            actions are stop, resume, delete, restart, and suspend.
        @type action: str
        @arg jobIDs:  Jobs to perform the action on.  
        @type jobIDs:  Either the string 'all' or a python sequence of 
            job identifiers.
        """
        
        # Validate the action
        actions = ('stop', 'suspend', 'resume', 'delete', 'restart')
        if action not in actions:
            raise InvalidAction(action)
            
        if jobIDs == 'all':
            # Delete all jobs
            self._updateJobs()
            jobList = self._jobIDs  # list of jobs to act on
        elif isinstance(jobIDs, tuple) or isinstance(jobIDs, list):
            # Delete some jobs
            jobList = jobIDs
        else:
            raise TypeError, jobIDs

        for jid in jobList:
            tempJob = Job(processID(jid), self._connection)
            tempJob.perform(action) # this will raise any errors
        
    def stopAll(self):
        """Stops all active jobs."""
        self.perform(action='stop', jobIDs='all')
        
    def suspendAll(self):
        """Suspends all active jobs."""
        self.perform(action='suspend', jobIDs='all')        

    def resumeAll(self):
        """Resumes all active jobs."""
        self.perform(action='resume', jobIDs='all')        

    def deleteAll(self):
        """Deletes all active jobs."""
        self.perform(action='delete', jobIDs='all')        

    def restartAll(self):
        """Restarts all active jobs."""
        self.perform(action='restart', jobIDs='all')

    def printJobs(self):
        """Prints information about all active Xgrid jobs."""
    
        self._updateJobs()
        print "##################################################"
        print "%-4s %-24s %-10s %-10s" % \
            ("id", "Date Started", "Status", "CPU Power")
        print "##################################################"
        for j in self._jobs:
            j.printInfo(0)

class GridManager:
    """Manage the grids of a given Xgrid controller."""
    
    def __init__(self, connection=None, update=0):
        """A class to manage a set of Xgrid grids.
        
        This class is meant to be a base class for the Controller class.
        It provides basic capabilities for listing the available grids.
        
        @arg connection:
            Instance of Connection class.  If empty a default Connection object
            is used.
        @type connection: Connection
        @arg update:  
            A boolean flag that determines whether or not the 
            internal state is updated upon creation.  This involves a call to 
            the Xgrid controller.
        @type update: boolean
        """
        
        if connection is None:
            self._connection = Connection()
        else:    
            self._connection = connection
            
        self._grids = []
        self._gridIDs = ()

        if update:
            self._updateGrids()

    def _updateGrids(self):
        """Updates the _gridIDs and _grids instance variables."""
         
        cmd = 'xgrid %s-grid list' % self._connection.connectString()
        result = xgridParse(cmd)
        self._gridIDs = result['gridList']
               
        # Now build the array of Grid objects
        self._grids = []
        for gridID in self._gridIDs:
            self._grids.append(Grid(gridID, self._connection)) 

    def grids(self, update=1):
        """Returns a list of initialized Grid objects.
        
        @arg update:  A boolean flag that determines whether or not the 
            internal state is updated upon creation.  This involves a call to 
            the Xgrid controller.
        @type update: boolean
        """

        if update:
            self._updateGrids()
        return self._grids

    def grid(self, gridID=u'0', update=1):
        """Returns the Grid object with grid identifier gridID.
                
        @arg gridID:
            The unicode string identifier of the grid.  If no gridID is given, 
            the default grid u'0' is used.
        @type gridID: unicode, int or str
        @arg update:  A boolean flag that determines whether or not the 
            internal state is updated upon creation.  This involves a call to 
            the Xgrid controller.
        @type update: boolean
        """
        
        processedGridID = processID(gridID)
            
        if update:
            self._updateGrids()
        if processedGridID in self._gridIDs:
            return Grid(processedGridID, self._connection)
        else:
            raise InvalidGridIdentifier(gridID)

    def gridIDs(self, update=1):
        """Returns a tuple of grid identifiers for all avialable grids.
        
        @arg update:  A boolean flag that determines whether or not the 
            internal state is updated upon creation.  This involves a call to 
            the Xgrid controller.
        @type update: boolean
        """
        
        if update:
            self._updateGrids()
        return self._gridIDs

class Controller(JobManager, GridManager):
    """A class for working with an Xgrid controller."""
    
    def __init__(self, connection=None, update=0):
        """This class provides an interface to an Xgrid controller.
        
        An Xgrid controller is a single machine that manages a set of
        of grids.  Each grid in turn, consists of a set of agents and
        jobs running on the agents.  
                
        This class provides access to the grids and jobs managed by the
        controller.  In Xgrid, both grids and jobs have identifiers, which are
        unicode strings, like u'0', but this module can take identifiers as
        strings or integers as well.  
        
        Controller and Grid objects can be used to submit Xgrid jobs, but the
        Job class is used to retrieve job results.
                
        The Controller is only the JobManager for the default Grid.  To access
        the jobs of other grids, create instances of their Grid objects.

        Examples
        ========
        
        >>> cn = Connection('myhost','mypassword')
        
        >>> c = Controller(cn)
        
        >>> c.jobIDs()
        (1, 2, 3)
        
        >>> j1 = c.job('1')     # Get an initialized Job object with id = '1'
        >>> j1
        <Job with id = 1>
        
        >>> c.grid_ids()        # List the grid ids
        ('0',)
        
        >>> c.grid('10')        # Get an initialized Grid object with id = '10'
        <Grid with gridID = 10>
        
        >>> c.grid()            # Get the Grid boject for the default grid        

        @arg connection: Instance of Connection class.  If empty a default 
            Connection object is used.
        @type connection: Connection
            
        @arg update:  A boolean flag that determines whether or not the 
            internal state is updated upon creation.  This involves a call to 
            the Xgrid controller.
        @type update: boolean    
        """
        JobManager.__init__(self, u'', connection)
        GridManager.__init__(self, connection)
            
        if update:
            self._update()
                  
    def _update(self):
        """Updates all instance variables for active grids and jobs."""
        
        self._updateGrids()
        self._updateJobs()
        
    # Job Submission
        
    def submit(self, cmd, args='', stdin='', indir='', email='', gridID=u'0'):
        """Submits a single task job to the specified grid.
        
        This is a nonblocking job submission method for a single job
        with no sub-tasks.  For more complicated jobs with sub-tasks, use
        the batch() method and the JobSpecification class.  
        
        Job results can be obtained by calling the results() method of the 
        Job object.
                
        @arg cmd:  
            The command the execute as a string.  The executable is not
            copied if the full path is given, otherwise it is.
        @type cmd: str            
        @arg args:  
            The command line arguments to be passed to the command.  
        @type args: list or str
        @arg stdin:
            A local file to use as the stdin stream for the job.
        @type stdin: str
        @arg indir:
            A local directory to copy to the remote agent.
        @type indir: str
        @arg email:
            An email to which notification will be send of various job
            state changes.
        @type email: str
        @arg gridID:
            The identifier of the Grid to which the job will be submitted.  
            If empty, the default grid u'0' is used.
        @type gridID: unicode, str or int
        @returns: Initialized Job object for sumbitted job.
        @rtype: Job                        
        """      
    
        j = Job(connection=self._connection)
        id = j.submit(cmd, args, stdin, indir, email, gridID)
        return j
        
    def batch(self, specification, gridID=u'0', silent=False):
        """Submits a batch job to the specified grid.
        
        This is a nonblocking job submission method used for submitting
        complex multi-task jobs.  For single task jobs, use submit().  
        
        To retrieve job results, use the results() method of the Job object. 
        
        @arg specification:
            The job specification of the job, which must be an instance of the 
            JobSpecification class.  See the docstring for JobSpecification 
            for more details.
        @type specification: JobSpecification
        @arg gridID:
            The identifier of the Grid to which the job will be submitted.  
            If empty, the default grid u'0' is used.
        @type gridID: unicode, str or int
        @arg silent:
            If set to True will slience all messages.
        @type silent: boolean   
        @returns: Initialized Job object for sumbitted job.
        @rtype: Job        
        """      
    
        j = Job(connection=self._connection)
        id = j.batch(specification, gridID, silent=silent)
        return j
        
class Grid(JobManager):
    """A class for working with jobs on a specific Xgrid grid."""
    
    def __init__(self, gridID=u'0', connection=None, update=0):
        """This class provides an interface to an Xgrid grid.
        
        An Xgrid grid is a collection of agents and jobs running on the
        agents.  This class provides access to the jobs running on a grid.
        Currently, Xgrid does not expose an API for working directly with
        the agents in a grid.
        
        Instances of this class can be obtained using two methods.
        
            1. By calling the grid() or grids() methods of the GridManager 
            or Controller classes.

            2. By creating a new Grid object directly with a valid gridID:
            
        >>> g = Grid(u'0')
            
        @arg gridID:
            The grid identifier of the grid. If gridID is empty the default 
            grid (u'0') will be used.
        @type gridID: unicode, int or str
        @arg connection:
            Instance of Connection class.  If empty a default Connection object
            is used.
        @type connection: Connection
        @arg update:  A boolean flag that determines whether or not the 
            internal state is updated upon creation.  This involves a call to 
            the Xgrid controller.
        @type update: boolean    
        """
        JobManager.__init__(self, gridID, connection)
        
        self._info = {}
        if update:
            self._update()
        
    # Private methods
        
    def _update(self):
        self._updateJobs()
        self._updateInfo()
            
    def _updateInfo(self):
        cmd = 'xgrid %s-grid attributes -gid %s' % \
            (self._connection.connectString(), self.gridID)
        result = xgridParse(cmd)
        self._checkGridID(result, self.gridID)
        self._info = result['gridAttributes']

    def _checkGridID(self, result, gridID):
        if result.has_key('error'):
            if result['error'] == 'InvalidGridIdentifier':
                raise InvalidGridIdentifier(gridID)

    def info(self, update=1):
        """Return the current status information about a grid.

        The grid info is a dictionary of keys describing the current state
        of the grid.

        @arg update:  A boolean flag that determines whether or not the
            internal state is updated upon creation.  This involves a call to
            the Xgrid controller.
        @type update: boolean

        """
        if update:
            self._updateInfo()
        return self._info

    # Job Submission
        
    def submit(self, cmd, args='', stdin='', indir='', email=''):
        """Submits a single task job to the current grid.
        
        This is a nonblocking job submission method for a single job
        with no sub-tasks.  For more complicated jobs with sub-tasks, use
        the batch() method and the JobSpecification class.  
        
        Job results can be obtained by calling the results() method of the 
        Job object.
                
        @arg cmd:  
            The command the execute as a string.  The executable is not
            copied if the full path is given, otherwise it is.
        @type cmd: str            
        @arg args:  
            The command line arguments to be passed to the command.  
        @type args: list or str
        @arg stdin:
            A local file to use as the stdin stream for the job.
        @type stdin: str
        @arg indir:
            A local directory to copy to the remote agent.
        @type indir: str
        @arg email:
            An email to which notification will be send of various job
            state changes.
        @type email: str
        @returns: Initialized Job object for sumbitted job.
        @rtype: Job                           
        """      

        j = Job(connection=self._connection)
        id = j.submit(cmd, args, stdin, indir, email, self.gridID)
        return j
        
    def batch(self, specification):
        """Submits a batch job to the current grid.
        
        This is a nonblocking job submission method used for submitting
        complex multi-task jobs.  For single task jobs, use submit().  
        
        To retrieve job results, use the results() method of the Job class. 
        
        @arg specification:
            The job specification of the job, which must be an instance of the 
            JobSpecification class.  See the docstring for JobSpecification 
            for more details.
        @type specification: JobSpecification
        @returns: Initialized Job object for sumbitted job.
        @rtype: Job        
        """  
            
        j = Job(connection=self._connection)
        id = j.batch(specification, self.gridID)
        return j
        
    # Other methods

    def __repr__(self):
        result = '<Grid with gridID = %s>' % self.gridID
        return result
        
class Job:
    """A class for working with an Xgrid job."""
    
    def __init__(self, jobID=u'999999999', connection=None):
        """An Xgrid job class.
        
        This class allows a user to work with an Xgrid job.  It provides
        capabilities for starting jobs, managing them and retrieving 
        their results.
        
        Job instances are created in two ways:
        
            1. By calling the job() or jobs() methods of the Grid or Controller
            classes.  
           
            2. By simply creating a new Job object:
          
        >>> j = Job(u'200')    # Create a new job with id of 200
        
        @arg jobID:
            The job identifier of the job.  To create a new job, leave blank.
        @type jobID: unicode, str or int
        @arg connection:
            Instance of Connection class.  If empty a default Connection object
            is used.
        @type connection: Connection
        """

        self.jobID = processID(jobID)

        if connection is None:
            self._connection = Connection()
        else:    
            self._connection = connection

        self._specification = {}
        self._info = {}

    # Semi-private methods

    def _updateInfo(self):
        cmd = 'xgrid %s-job attributes -id %s' % \
            (self._connection.connectString(), self.jobID)
        result = xgridParse(cmd)
        self._checkJobID(result)
        self._info = result['jobAttributes']
        
    def _updateSpecification(self):
        cmd = 'xgrid %s-job specification -id %s' % \
            (self._connection.connectString(), self.jobID)
        result = xgridParse(cmd)
        self._checkJobID(result)
        self._specification = result['jobSpecification']
        
    def _update(self):
        self._updateInfo()
        self._updateSpecification()

    def _checkJobID(self, result):
        if result.has_key('error'):
            if result['error'] == 'InvalidJobIdentifier':
                raise InvalidJobIdentifier(self.jobID)

    def _checkGridID(self, result, gridID):
        if result.has_key('error'):
            if result['error'] == 'InvalidGridIdentifier':
                raise InvalidGridIdentifier(gridID)
                         
    # Get methods
                                                      
    def specification(self, update=1):
        """Return the Xgrid job specification.
        
        The Xgrid job specification is the dictionary that Xgrid uses
        to submit the job. It contains keys that describe the command
        arguments, directories, etc.
        """
        
        if update:
            self._updateSpecification()
        return self._specification
        
    def info(self, update=1):
        """Return the current status information about a job.
        
        The job info is a dictionary of keys describing the current state
        of the job. This includes start/stop dates, name, etc.
        
        The method printInfo() prints the info() dictionary in a nice form.
        
        @arg update:  A boolean flag that determines whether or not the 
            internal state is updated upon creation.  This involves a call to 
            the Xgrid controller.
        @type update: boolean    

        """
        
        if update:
            self._updateInfo()
        return self._info
        
    # Job submission and results

    def results(self, stdout='', outdir='', stderr='', block=10, silent=False):
        """Retrieve the results of an Xgrid job.
        
        This method provides both a blocking and nonblocking method of 
        getting the results of an Xgrid job. The job does not need to be 
        completed to retrieve the results. Because of this, the results 
        method can be used to get partial results while the job continues 
        to run. It can also automatically name output files.
        
        @arg stdout:
            The local file in which to put the stdout stream of the remote job.
            If this is empty, the method will automatically generate a name in
            the local directory of the form: xgridjob-jobID.out. This file 
            always is placed in the cwd rather than the outdir
        @type stdout: str   
        @arg stderr:
            The local file in which to put the stderr stream of the remote job.
            If this is empty, the method will automatically generate a name in
            the local directory of the form: xgridjob-jobID.err.  
        @type stderr: str
        @arg outdir:
            The local directory in which to put the files retrieved from the
            remote job. This is only for files other than the stdout and
            stderr files. When empty, the other files are not brought back.
            This is to prevent any accidental overwrites of results.
        @type outdir: str
        @arg block:
            Whether or not to block until the job is finished. If block=0, 
            partially completed results are retrieved and the job will
            continue to run. If block > 0, the job status is queried every
            block seconds and the results are returned when the job 
            is completed.
        @type block: int
        @arg silent: Silence all messages.
        @type silent: boolean            
        """
            
        so = ''
        se = ''
        out = ''
        
        if stdout:
            so = '-so ' + stdout + ' '
        else:
            temp_stdout = 'xgridjob-' + self.jobID + '.out'
            so = '-so ' + temp_stdout + ' '
            
        if outdir:
            out = '-out ' + outdir
            
        if stderr:
            se = '-se ' + stderr + ' '
        else:
            temp_stderr = 'xgridjob-' + self.jobID + '.err'
            se = '-se ' + temp_stderr + ' '        
            
        cmd = "xgrid %s-job results -id %s %s%s%s" % \
            (self._connection.connectString(),self.jobID,so,se,out)
        
        # Block until the results are back! 
        self._updateInfo()
        if block:
            while not self._info['jobStatus'] == 'Finished':
                time.sleep(block)
                self._updateInfo()
            log = xgridParse(cmd)
        else:
            log = xgridParse(cmd)
            
        if (not silent) and (len(so) > 0):
            print "Job stdout saved in file: " + so[4:]
                    
    # Job Submission

    def submit(self, cmd, args='', stdin='', indir='', email='', gridID=u'0', 
               silent=False): 
        """Submits a single task job to the specified grid.
        
        This is a nonblocking job submission method for a single job
        with no sub-tasks.  For more complicated jobs with sub-tasks, use
        the batch() method.  
        
        Job results can be obtained by calling the results() method.
                
        @arg cmd:  
            The command to execute as a string.  The executable is not
            copied if the full path is given, otherwise it is.
        @type cmd: str            
        @arg args:  
            The command line arguments to be passed to the command.  
        @type args: list or str
        @arg stdin:
            A local file to use as the stdin stream for the job.
        @type stdin: str
        @arg indir:
            A local directory to copy to the remote agent.
        @type indir: str
        @arg email:
            An email to which notification will be send of various job
            state changes.
        @type email: str
        @arg gridID:
            The identifier of the Grid to which the job will be submitted.  
            If empty, the default grid u'0' is used.
        @type gridID: unicode, str or int
        @arg silent:
            If set to True will slience all messages.
        @type silent: boolean         
        @returns: Initialized Job object for sumbitted job.
        @rtype: Job                           
        """      

        processedGridID = processID(gridID)
            
        # First build the submit_string
        submitString = ''
        stdinString = ''
        indirString = ''
        emailString = ''
        if stdin:
            stdinString = '-si ' + stdin + ' '
        if indir:
            indirString = '-in ' + indir + ' '
        if email:
            emailString = '-email ' + email + ' '

        # Process the arguments
        if isinstance(args, str):
            argString = args
        elif isinstance(args, list):
            argList = []
            for a in args:
                argList.append(str(a)+" ")
            argString = "".join(argList).strip()
        else:
            raise TypeError
        
        submitString = stdinString + indirString + emailString + \
            cmd + ' ' + argString
        
        # Now submit the job and set the job id
        #print "Submitting job to grid: ", gridID
        cmd = "xgrid %s-gid %s -job submit %s" % \
            (self._connection.connectString(), gridID, submitString)
        jobinfo = xgridParse(cmd)
        self._checkGridID(jobinfo, processedGridID) 
        self.jobID = jobinfo['jobIdentifier']
        if not silent:
            print "Job submitted with id: ", self.jobID
        return self.jobID
        
    def batch(self, specification, gridID=u'0', silent=False):
        """Submits a batch job to the specified grid.
        
        This is a nonblocking job submission method used for submitting
        complex multi-task jobs. For single task jobs, use submit(). 
        
        To retrieve job results, use the results() method.
         
        @arg specification:
            The job specification of the job, which must be an instance of the 
            JobSpecification class. See the docstring for JobSpecification 
            for more details.
        @type specification: JobSpecification
        @arg gridID:
            The identifier of the Grid to which the job will be submitted.  
            If empty, the default grid u'0' is used.
        @type gridID: unicode, str or int
        @arg silent:
            If set to True will slience all messages.
        @type silent: boolean   
        @returns: Initialized Job object for sumbitted job.
        @rtype: Job        
        """      

        if not isinstance(specification, JobSpecification):
            raise XgridError

        processedGridID = processID(gridID)

        #job_dict = propertyListFromPythonCollection(specification.jobspec())
        jobSpec = specification.jobSpec()
        plistFile = tempfile.NamedTemporaryFile().name
        jobSpec.writeToFile_atomically_(plistFile, 1)
        cmd = "xgrid %s-gid %s -job batch %s" % \
            (self._connection.connectString(), processedGridID, plistFile)
        jobinfo = xgridParse(cmd)
        self._checkGridID(jobinfo, processedGridID)
        self.jobID = jobinfo['jobIdentifier']
        
        if not silent:
            print "Job submitted with id: ", self.jobID
        
        return self.jobID


    # Job control methods
            
    def perform(self, action):
        """Performs an action on a job.
        
        @arg action:
            The action to be performed as a string.  Implemented actions
            are stop, resume, delete, restart, and suspend.
        @type action: str
        """

        actions = ('stop', 'suspend', 'resume', 'delete', 'restart')
        if action in actions:
            cmd = 'xgrid %s-job %s -id %s' % \
                (self._connection.connectString(), action, self.jobID)           
            result = xgridParse(cmd)
            self._checkJobID(result)
            print "Action %s performed on job %s" % (action, self.jobID)
            # If delete reset everything but the controller
            if action == 'delete':
                self.jobID = u'999999999'
                self._specification = {}
                self._info = {}
        else:
            raise InvalidAction(action)
            
    def stop(self):
        """Stops the job."""
        self.perform('stop')
        
    def suspend(self):
        """Suspends the job."""
        self.perform('suspend')
        
    def resume(self):
        """Resumes the job."""
        self.perform('resume')
        
    def delete(self):
        """Deletes the job."""
        self.perform('delete')

    def restart(self):
        """Restarts the job."""
        self.perform('restart')
        
    # Other methods
    
    def __repr__(self):
        result = '<Job with jobID = %s>' % self.jobID
        return result
        
    def printInfo(self, verbose=True):
        """Prints the info() dictionary of a job."""
        self._updateInfo()
        if verbose == False:
            output = "%-4s %-24s %-10s %-10s" % \
                (self.jobID, self._info['dateStarted'],
                 self._info['jobStatus'],
                 self._info['activeCPUPower'])
            print output
        elif verbose == True:
            print "{"
            for key in self._info.keys():
                print '   ', key, ': ', self._info[key]
            print "}"
    
    def printSpecification(self):
        """Print the job specification used to submit the job."""
            
        self._updateSpecification()
        print "{"
        for key in self._specification.keys():
            print '   ', key, ': ', self._specification[key]
        print "}"
        
class JobSpecification:
    """A class used for constructing multi-task batch jobs."""
    
    def __init__(self):
        """This class is used to setup the plist file for multi-task jobs.
        """
        self._jobDict = Foundation.NSMutableDictionary.dictionaryWithCapacity_(10)
        self._jobSpec = Foundation.NSArray.arrayWithObject_(self._jobDict)
        self._jobDict[u'taskSpecifications'] = {}
        #self.tasks = []
        self.nextTask = 0
        self._jobDict[u'applicationIdentifier'] = u'PyXG'
        self._jobDict[u'schedulerParameters'] = {}
        self._jobDict[u'schedulerParameters'][u'tasksMustStartSimultaneously'] \
            = u'NO'
        
    # Utility methods

    def _checkSchedulerParameters(self):
        if not self._jobDict.has_key(u'schedulerParameters'):
            self._jobDict[u'schedulerParameters'] = {}
                            
    def _checkInputFiles(self):
        if not self._jobDict.has_key(u'inputFiles'):
            self._jobDict[u'inputFiles'] = {}

    def jobSpec(self):
        """Prints the full job specification dictionary."""
        return self._jobSpec

    # Job/Task setup methods
        
    def setName(self, name):
        """Set the name (a string) of the job."""
        self._jobDict[u'name'] = unicode(name)
        
    def name(self):
        """Returns the job name."""
        return self._jobDict.get(u'name')
        
    def setEmail(self, email):
        """Set the notification email for the batch job."""
        self._jobDict[u'notificationEmail'] = unicode(email)
        
    def email(self):
        """Returns the notification email."""
        return self._jobDict.get(u'notificationEmail')
        
    def setTasksMustStartSimultaneously(self, simul):
        """Sets the tasksMustStartSimultanously flag."""

        if(simul):
            self._jobDict[u'schedulerParameters'][u'tasksMustStartSimultaneously'] = u'YES'
        else:
            self._jobDict[u'schedulerParameters'][u'tasksMustStartSimultaneously'] = u'NO'
    
    def tasksMustStartSimultaneously(self):
        """Returns the value of tasksMustStartSimultaneously."""
        return self._jobDict[u'schedulerParameters'].get(u'tasksMustStartSimultaneously')
            
    def setMinimumTaskCount(self, count):
        """Sets the min number of tasks that should be started."""
        #self._checkSchedulerParameters()
        self._jobDict[u'schedulerParameters'][u'minimumTaskCount'] = count
    
    def minimumTaskCount(self):
        """Returns the value of minimumTaskCount."""
        return self._jobDict[u'schedulerParameters'].get(u'minimumTaskCount')
        
    def setDependsOnJobs(self, jobArray):
        """Takes a list of Xgrid job ids that must complete before this job begins."""
        #self._checkSchedulerParameters()
        self._jobDict[u'schedulerParameters'][u'dependsOnJobs'] = \
            [unicode(j) for j in jobArray]
        
    def dependsOnJobs(self):
        """Returns the value of dependsOnJobs."""
        return self._jobDict[u'schedulerParameters'].get(u'dependsOnJobs') 
        
    def addFile(self, localFilePath, fileName, isExecutable=0):
        """Specifies a local file to copy to the Xgrid agents.
        
        This file is encoded into a base64 string and inserted into the
        job specification dictionary.
        
        @arg localFilePath:
            The full path of the file on the client (local) computer
        @type localFilePath: unicode or str
        @arg fileName:
            The name to call the file on the agent
        @type fileName: unicode or str
        @arg isExecutable:
            Set to 1 if the file should be executable
        @type isExecutable: boolean
        """
        
        assert os.path.isfile(localFilePath), "File does not exist: %s" % localFilePath
        path = NSString.stringWithString_(unicode(localFilePath)).stringByStandardizingPath()
        data = Foundation.NSData.dataWithContentsOfFile_(path)
        self._checkInputFiles()
        if isExecutable:
            isExecString = u'YES'
        else:
            isExecString = u'NO'
        self._jobDict[u'inputFiles'][unicode(fileName)] = \
            {u'fileData':data,u'isExecutable':isExecString}
    
    def delFile(self, fileName):
        """Deletes the file named fileName from the JobSpecification.
        
        List filenames using the flies() method.
        """
        if self._jobDict.has_key(u'inputFiles'):
            if self._jobDict[u'inputFiles'].has_key(unicode(fileName)):
                del self._jobDict[u'inputFiles'][unicode(fileName)]
                
    def files(self):
        """Prints a list of included filenames."""
        f = self._jobDict.get(u'inputFiles')
        if f:
            return f.keys()
            
    def addTask(self, cmd, args=u'', env={}, inputStream=u'', \
        dependsOnTasks=[]):
        """Adds a task to the jobSpecification.
        
        @arg cmd:  
            The command to execute as a string.  The executable is not
            copied if the full path is given, otherwise it is.
        @type cmd: str            
        @arg args:  
            The command line arguments to be passed to the command.  
        @type args: list or str
        @arg env:
            A Python dictionary of environment variables to use on the agents.
        @type env: unicode or str
        @arg inputStream:
            A local file to send to the agents that will be used as stdin for
            the task
        @type inputStream: unicode or str
        @arg dependsOnTasks:
            A list of task ids that must complete before this one begins
        @type dependsOnTasks: list
        """
        taskSpec = {}
        taskName = unicode('task%i' % self.nextTask)
        self.nextTask += 1
        
        # Process the arguments
        if isinstance(args, str) or isinstance(args, unicode):
            argList = args.split(' ')
        elif isinstance(args, list):
            argList = args
        else:
            raise TypeError
        
        taskSpec[u'command'] = unicode(cmd)
        if args:
            taskSpec[u'arguments'] = [unicode(a) for a in argList]
        if env:
            taskSpec[u'environment'] = env
        if inputStream:
            taskSpec[u'inputStream'] = unicode(inputStream)
        if dependsOnTasks:
            taskSpec[u'dependsOnTasks'] = dependsOnTasks
        self._jobDict[u'taskSpecifications'][taskName] = taskSpec
        
    def copyTask(self):
        pass
        
    def delTask(self, task):
        """Deletes the task named task.
        
        List the task names using the tasks() method.
        """
        if self._jobDict[u'taskSpecifications'].has_key(unicode(task)):
            del self._jobDict[u'taskSpecifications'][unicode(task)]        
        
    def editTask(self):
        pass
        
    def tasks(self):
        """Return a list of the task names."""
        return self._jobDict[u'taskSpecifications'].keys()    
        
    def printTasks(self):
        """Print the task specifications of all tasks."""
        for tid in self._jobDict[u'taskSpecifications'].keys():
            print str(tid) + "  " + str(self._jobDict[u'taskSpecifications'][tid])

