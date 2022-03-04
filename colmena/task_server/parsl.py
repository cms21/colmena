"""Parsl task server and related utilities"""
import os
import shlex
import logging
import platform
from concurrent.futures import Future
from functools import partial, update_wrapper
from pathlib import Path
from tempfile import mkdtemp
from time import perf_counter
from datetime import datetime
from typing import Optional, List, Callable, Tuple, Dict, Union

import parsl
from parsl.app.app import AppBase
from parsl.app.bash import BashApp
from parsl.config import Config
from parsl.app.python import PythonApp

from colmena.models import Result, ExecutableTask, FailureInformation
from colmena.proxy import resolve_proxies_async
from colmena.redis.queue import TaskServerQueues
from colmena.task_server.base import run_and_record_timing, FutureBasedTaskServer

logger = logging.getLogger(__name__)


# Functions related to splitting "ExecutableTasks" into multiple steps
def _execute_preprocess(task: ExecutableTask, result: Result) -> Tuple[Result, Path, Tuple[List[str], Optional[str]]]:
    """Perform the pre-processing step for an executable task

    Must execute on the remote system

    Manages pulling inputs down from the ProxyStore,
     creating a temporary run directory,
     and calling ExecutableTask.preprocess to store
     any needed items in that run directory

    Args:
        task: Description of task to be executed
        result: Object holding the inputs
    """

    # Mark that compute has started
    result.mark_compute_started()

    # Unpack the inputs
    result.time_deserialize_inputs = result.deserialize()

    # Start resolving any proxies in the input asynchronously
    start_time = perf_counter()
    resolve_proxies_async(result.args)
    resolve_proxies_async(result.kwargs)
    result.time_async_resolve_proxies = perf_counter() - start_time

    # Create a temporary directory
    #  TODO (wardlt): Figure out how to allow users to define a path for temporary directories
    temp_dir = Path(mkdtemp(prefix='colmena_'))

    # Execute the function
    start_time = perf_counter()
    try:
        output = task.preprocess(temp_dir, result.args, result.kwargs)
    except BaseException as e:
        output = None
        result.success = False
        result.failure_info = FailureInformation.from_exception(e)
    finally:
        end_time = perf_counter()

    result.additional_timing['exec_preprocess'] = end_time - start_time

    return result, temp_dir, output


def _execute_postprocess(task: ExecutableTask, exec_time: float, result: Result, temp_dir: Path):
    """Execute the post-processing function after an executable completes

    Args:
        task: Task description, which contains details on how to post-process the results
        exec_time: Output from the exec function
        result: Storage for the result data
        temp_dir: Path to the run directory on the remote system
    """

    # Store the run time in the result object
    result.additional_timing['exec_execution'] = exec_time

    # Execute the function
    start_time = perf_counter()
    try:
        output = task.postprocess(temp_dir)
        result.success = True
    except BaseException as e:
        output = None
        result.success = False
        result.failure_info = FailureInformation.from_exception(e)
    finally:
        end_time = perf_counter()
    result.additional_timing['exec_postprocess'] = end_time - start_time

    # Store the results
    if result.success:
        result.set_result(output, datetime.now().timestamp() - result.time_compute_started)

    # Add the worker information into the tasks, if available
    worker_info = {'hostname': platform.node()}
    result.worker_info = worker_info

    # Re-pack the results (will use proxystore, if able)
    result.time_serialize_results = result.serialize()

    return result


def _execute_execute(task: ExecutableTask, task_path: Path, arguments: List[str], stdin: Optional[str], *,
                     stdout: str, stderr: str, pre_exec: str = None, nthrd: int = 0, ngpus: int = 0) -> str:
    """Execute the executable step of an executable task

    This function is executed after :meth:`__execute_preprocess` has completed, which means
    any necessary input files are in `task_path` and any necessary CLI arguments have been determined.

    It will be executed as a :class:`BashApp`, so it returns a run command given the arguments provided.

    The kwargs for the argument include resource requirements and other information used to communicate
    the task requirements to the Parsl Executor.

    Args:
        task: General task information. Includes the path to the executable
        task_path: Path to the run directory
        arguments: List of arguments to add to the function execution
        stdin: Data to be passed to the stdin (not currently supported)
        pre_exec: List of environment variables to set
        stdout: Path to the stdout for the function (should be ``task_task_path // 'colmena.stdout`).
            Provided as a kwargs so that the Parsl executor knows where to write the file
        stderr: Path to the stderr for the function (should be ``task_task_path // 'colmena.stdout`).
            Provided as a kwargs so that the Parsl executor knows where to write the file
        nthrd: Number of threads to use per node
        ngpus: Number of GPUs to use per node
    Returns:
        The function to invoke as a string
    """

    assert stdin is None or len(stdin) == 0, "Standard in is not supported yet"

    # Create the shell command
    #  TODO (wardlt): This is shlex.join, which is only available in Py3.8+
    shell_cmd = " ".join(shlex.quote(str(s)) for s in task.executable + arguments)

    # Move to the run directory
    os.chdir(task_path)

    return shell_cmd


def _preprocess_callback(
        preprocess_future: Future,
        result: Result,
        task_server: 'ParslTaskServer',
        topic: str,
        execute_fun: AppBase,
        postprocess_fun: AppBase
):
    """Perform the next steps in an executable workflow

    If preprocessing was unsuccessful, send failed result back to client.
    If successful, submit the "execute task" to Parsl and attach a callback
    to that function which will submit the postprocess task.

    Args:
        preprocess_future: Future provided when submitting pre-process task to Parsl
        result: Result object to be gradually updated
        task_server: Connection to the Parsl task server. Used to send results back to client
        topic: Topic of the task
        execute_fun: Parsl App that submits the execution task
        postprocess_fun: Parsl App that performs post-processing
    """

    # If the Parsl execution was unsuccessful, send the result back to the client
    if preprocess_future.exception() is not None:
        return task_server.perform_callback(preprocess_future, result, topic)

    # If successful, unpack the outputs
    result, temp_dir, (exec_args, exec_stdin) = preprocess_future.result()

    # If unsuccessful, send the results back to the client
    if result.success is not None and not result.success:
        logger.info('Result failed during preprocessing. Sending back to client.')
        result.time_running = result.additional_timing.get('exec_preprocess', 0)
        return task_server.queues.send_result(result, topic)

    # If successful, submit the execute step and pass its result to Parsl
    logger.info(f'Preprocessing was successful for {result.method} task. Submitting to execute')
    exec_future: Future = execute_fun(temp_dir, exec_args, exec_stdin,
                                      stdout=str(temp_dir / 'colmena.stdout'),
                                      stderr=str(temp_dir / 'colmena.stderr'))

    # Submit post-process to follow up
    post_future: Future = postprocess_fun(exec_future, result, temp_dir)
    post_future.add_done_callback(lambda x: task_server.perform_callback(x, result, topic))


class ParslTaskServer(FutureBasedTaskServer):
    """Task server based on Parsl

    Create a Parsl task server by first creating a resource configuration following
    the recommendations in `the Parsl documentation
    <https://parsl.readthedocs.io/en/stable/userguide/configuring.html>`_.
    Then instantiate a task server with a list of Python functions,
    configurations defining on which Parsl executors each function can run,
    and the Parsl resource configuration.
    The executor(s) for each function can be defined with a combination
    of per method specifications

    .. code-block:: python

        ParslTaskServer([(f, {'executors': ['a']})], queues, config)

    and also using a default executor

    .. code-block:: python

        ParslTaskServer([f], queues, config, default_executors=['a'])

    Further configuration options for each method can be defined
    in the list of methods.

    **Technical Details**

    The task server stores each of the supplied methods as Parsl "PythonApp" classes.
    Tasks are launched using these PythonApps after being received on the queue.
    The Future provided when requesting the method invocation is then passed
    to second PythonApp that pushes the result of the function to the output
    queue after it completes.
    That second, "output_result," function runs on threads of the same
    process as this task server.
    There is also a separate thread that monitors for Futures that yield an error
    before the "output_result" function and sends back the error messages.
    """

    def __init__(self, methods: List[Union[Callable, Tuple[Callable, Dict]]],
                 queues: TaskServerQueues,
                 config: Config,
                 timeout: Optional[int] = None,
                 default_executors: Union[str, List[str]] = 'all'):
        """

        Args:
            methods (list): List of methods to be served.
                Each element in the list is either a function or a tuple where the first element
                is a function and the second is a dictionary of the arguments being used to create
                the Parsl ParslApp see `Parsl documentation
                <https://parsl.readthedocs.io/en/stable/stubs/parsl.app.app.python_app.html#parsl.app.app.python_app>`_.
            queues (TaskServerQueues): Queues for the task server
            config: Parsl configuration
            timeout (int): Timeout, if desired
            default_executors: Executor or list of executors to use by default.
        """
        super().__init__(queues, timeout)

        # Insert _output_workers to the thread count
        executors = config.executors.copy()
        config.executors = executors

        # Get a list of default executors that _does not_ include the output workers
        if default_executors == 'all':
            default_executors = [e.label for e in executors]

        # Store the Parsl configuration
        self.config = config

        # Assemble the list of methods
        self.methods_: Dict[str, Tuple[AppBase, str]] = {}  # Store the method and its type
        self.exec_apps_: Dict[str, Tuple[AppBase, AppBase]] = {}  # Stores the execute and post-process apps for ExecutableTasks
        for method in methods:
            # Get the options or use the defaults
            if isinstance(method, (tuple, list)):
                if len(method) != 2:
                    raise ValueError('Method description should a tuple of length 2')
                function, options = method
            else:
                function = method
                options = {'executors': default_executors}
                logger.info(f'Using default executors for {function.__name__}: {default_executors}')

            # Make the Parsl app
            name = function.__name__

            # If the function is an executable, just wrap it
            if not isinstance(function, ExecutableTask):
                wrapped_function = partial(run_and_record_timing, function)
                wrapped_function = update_wrapper(wrapped_function, function)
                app = PythonApp(wrapped_function, **options)
                self.methods_[name] = (app, 'basic')
            else:
                logger.info(f'Building a chain of apps for an ExecutableTask, {function.__name__}')
                # If it is an executable, the function we launch initially is a "preprocess inputs" function
                preprocess_fun = partial(_execute_preprocess, function)
                preprocess_fun.__name__ = f'{name}_preprocess'
                preprocess_app = PythonApp(preprocess_fun, **options)

                # Make executable app, which is just to perform the execute
                execute_fun = partial(_execute_execute, function)
                execute_fun.__name__ = f'{name}_execute'
                execute_app = BashApp(execute_fun, **options)

                # Make the post-process app, which gathers the results and puts them in the "result object"
                postprocess_fun = partial(_execute_postprocess, function)
                postprocess_fun.__name__ = f'{name}_postprocess'
                postprocess_app = PythonApp(postprocess_fun, **options)

                # Store them for use during submission phase
                self.methods_[name] = (preprocess_app, 'exec')
                self.exec_apps_[name] = (execute_app, postprocess_app)

        logger.info(f'Defined {len(self.methods_)} methods: {", ".join(self.methods_.keys())}')

        # If only one method, store a default method
        self.default_method_ = list(self.methods_.keys())[0] if len(self.methods_) == 1 else None
        if self.default_method_ is not None:
            logger.info(f'There is only one method, so we are using {self.default_method_} as a default')

    def _submit(self, task: Result, topic: str) -> Optional[Future]:
        # Determine which method to run
        if self.default_method_ and task.method is None:
            method = self.default_method_
        else:
            method = task.method

        # Submit the application
        function, func_type = self.methods_[method]
        future: Future = function(task)
        logger.debug('Pushed task to Parsl')
        # TODO (wardlt): Implement "resubmit if task returns a new future." or the ability to launch Parsl workflows with >1 step

        # Depending on the task type, return a different future
        if func_type == 'basic':
            # For most functions, just return the future so the task server will handle the output
            return future
        elif func_type == 'exec':
            # For executable functions, we have a different route for returning results
            exec_app, post_app = self.exec_apps_[method]
            future.add_done_callback(lambda x: _preprocess_callback(x, task, self, topic, exec_app, post_app))
            return None  # `None` prevents the Task Server from adding its own callback
        else:
            raise ValueError(f'Unrecognized function type: {func_type}')

    def _cleanup(self):
        """Close out any resources needed by the task server"""
        # Wait until all tasks have finished
        dfk = parsl.dfk()
        dfk.wait_for_current_tasks()
        logger.info(f"All tasks have completed for {self.__class__.__name__} on {self.ident}")

    def run(self) -> None:
        # Launch the Parsl workflow engine
        parsl.load(self.config)
        logger.info(f"Launched Parsl DFK. Process id: {os.getpid()}")

        # Start the loop
        super().run()
