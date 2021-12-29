from __future__ import annotations

import importlib.util
import traceback
from scheduler.enums import Fields
import asyncio as aio
from functools import partial


async def python_evaluator(path: str, command: str, arguments: dict | None = {}, cache = None,
                           timeout: float | int | None = None) -> int:
    """
    Evaluate a python function in module specified by path. Can pass in arguments
    into the python function and also pass a reference to the scheduler cache.
    

    Parameters
    ----------
    command : str
        The function name.
    arguments : Dict | None, optional
        Arguments to be passed into the function. The default is {}.
        
    path : str
        Module full path. Must be a normalized absolutized path.
    cache :  Cache, optional
        A reference to the scheduler cache. The default is None.
    timeout: float | int | None, optional
        Specify a time limit for the python function execution. The default is None.

    Returns
    -------
    int
        Return code/value from the python function. Expects the python function
        to return an integer.
    str
        Return string output describing the evaluation of the given function
    

    """
    module_path = path.split("//")
    spec = importlib.util.spec_from_file_location(module_path[-1][:-3], path)
    lib = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lib)
    func = partial(getattr(lib, command), **arguments, **{Fields.cache.value: cache})

    loop = aio.get_event_loop()
    try:
        future = loop.run_in_executor(None, func) # run_in_executor does not take kwargs
        await aio.wait_for(fut=future, timeout=timeout, loop=loop)
        return 0, "Function {} ran successfully".format(command)
    except Exception as e:
        return 1, traceback.format_exc()
    

async def cmd_evaluator(command: str, arguments: str = "", timeout: float | int | None = None) -> tuple[int, str]:
    
    """
    Evaluates a command line expression. Can pass arguments alongside the 
    expression.

    Parameters
    ----------
    command : str
        The command line expression/executable
    arguments : str, optional
        Arguments to be passed into the function. The default is "".
    timeout: float | int | None, optional
        Specify a time limit for the command line executable. The default is None.
    log_error: bool, optional
        Choose whether or not to log errors/failed runs.

    Returns
    -------
    int
        Return code/value from running the cmd commands.
    str
        Return string output describing the evaluation of the given cmd commands

    """
    cmd_string = command + " " + arguments

    proc = await aio.create_subprocess_shell(cmd_string, stdout=aio.subprocess.PIPE,
                                             stderr=aio.subprocess.PIPE)

    stdout, stderr = await aio.wait_for(proc.communicate(), timeout=timeout)

    encoding = "utf-8"
    logging_info = ""
    if stdout != "": logging_info = logging_info + stdout.decode(encoding)
    if stderr != "": logging_info = logging_info + stderr.decode(encoding)
    return proc.returncode, logging_info

