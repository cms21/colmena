"""Support for a Balsam task server"""
from dataclasses import dataclass
from threading import Thread
from typing import Dict, Optional, Any
from time import sleep
from uuid import uuid4
import logging

from colmena.models import Result
from colmena.redis.queue import ClientQueues, TaskServerQueues
from colmena.task_server.base import BaseTaskServer

from balsam.api import Job,Site,BatchJob,ApplicationDefinition
from balsam._api.models import App

logger = logging.getLogger(__name__)

# TODO (wardlt): This is non-functional stub that must be filled in
class BalsamTaskServer(BaseTaskServer):
    """Implementation of a task server which executes applications registered with a Balsam workflow database"""

    def __init__(self,
                 #methods: List[str], # a list of apps registered in the Balsam site
                 num_nodes: int, # Number of nodes to request for the BatchJob, if we use elastic queuing, this could change
                 queues: TaskServerQueues, 
                 pull_frequency: float,
                 balsam_site: str,
                 method_aliases: Optional[Dict[str, str]] = None,
                 timeout: Optional[int] = None):
        """
        Args:
            aliases: Mapping between simple names and names of application for Balsam
            pull_frequency: How often to check for new tasks
            queues: Queues used to communicate with thinker
            timeout: Timeout for requests from the task queue
        """
        super().__init__(queues, timeout)
        self.method_aliases = method_aliases.copy()
        self.pull_frequency = pull_frequency
        self.num_nodes = num_nodes

        # Ongoing tasks
        self.ongoing_tasks: Dict[str, Result] = dict() #Is this running tasks?

        # Placeholder for a client objects
        self.balsam_site = balsam_site

        # Check that the apps are registered in the site
        site = Site.objects.get(name=self.balsam_site)
        registered_apps = [a.name for a in App.objects.filter(site_id=site.id)]
        if len(registered_apps)== 0:
            raise ValueError(f"No apps registered in Balsam site {site.name}")
        for method in method_aliases:
            if method_aliases[method] not in registered_apps:
                raise ValueError(f'Method {method_aliases[method]} not registered in Balsam site {site.name}')

    def process_queue(self, topic: str, task: Result):
        # TODO (wardlt): Send the task to Balsam

        # Get the name of the method
        #app_name should be a string that the app is registered under in the balsam site
        #I'm assuming that task.method is a string
        app_name = self.method_aliases[task.method]
        job = Job(app_id=app_name, 
                site_name=self.balsam_site,
                workdir=str(task.task_id), #this needs to be a unique path where balsam will run the task.  It is relative to data within the site.
                parameters=task.inputs,
                tags={'topic': topic, 'server_id': self.server_id, 'colmena_task_id': task.task_id, 'returned_to_colmena':False},
                num_nodes=task.resources.node_count,
                #node_packing_count=? If we want more than one task to run on a node we need to set this
                ranks_per_node=task.resources.cpu_processes,
                threads_per_rank=task.resources.cpu_threads,
                #threads_per_core=? I'm going to assume that cpu_threads is defined per MPI rank, not physical core
                #gpus_per_rank=? how does colmena define the number of gpus per rank?
                )
        job.save()
        task_id = job.id
        logger.info(f'Submitted a {app_name} task to Balsam: {task_id}')

    def _query_results(self):
        """Runs and gets results that were submitted by th"""

        while True:
            # Query Balsam for completed tasks
            sleep(self.pull_frequency)
            #get the site id
            site = Site.objects.get(name=self.balsam_site)
            #use the filtering function to identify finished jobs in the site
            new_jobs = Job.objects.filter(site_id=site.id,state='JOB_FINISHED',tags={'returned_to_colmena':False})
            # Send the completed tasks back
            for job in new_jobs:
                # Get the associated Colmena task
                result = self.ongoing_tasks.pop(job.tags['colmena_task_id'])
                job.tags['returned_to_colmena'] = True

                # Add the result to the Colmena message and serialize it all
                #I'm not sure what form this result takes and how to pass it
                #I'm also uncertain how runtime should be defined.
                #Should it be the time between when the job enters the CREATED state to the JOB_FINISHED state 
                # or the time in the RUNNING state?  Either way we would access the events to get this info
                result.set_result(job.result(timeout=self.timeout)) 
                result.serialize()

                # Send it back to the client
                self.queues.send_result(result, job.tags['topic'])

    def _setup(self): 
        # TODO (wardlt): Prepare to send and receive tasks from Balsam
        # Connect to Balsam
        #I think this is equivalent to starting the balsam site? (e.g. %balsam site start).  This is typically done at the command line.  
        #self.balsam_client = self.login_creds
        #Instead, I'll get the site object from the name
        site = Site.objects.get(name=self.balsam_site)

        # Launch a thread
        #return_thread = Thread(target=self._query_results, daemon=True)
        #return_thread.start()
        
        #I'm not sure, but is this where we should start the launcher?
        BatchJob.objects.create(num_nodes=self.num_nodes,
                                wall_time_min=10,
                                queue=self.queues.queue, #uncertain about this
                                project=self.queues.project,#uncertain about this
                                site_id=site.id,
                                job_mode="mpi"
                                )


