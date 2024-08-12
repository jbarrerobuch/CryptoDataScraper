from . import Agent
import time
import memory_profiler as mp

def profile_function(func:callable, agent:Agent.Collector, args:tuple=(), kwargs:dict={}) -> tuple:
    """
    Measures the memory usage of a specified function, updates the memory\n
    usage and the time used in seconds in the agent and returns the\n
    function's return value.

    Args:
        func (callable): The function to measure.
        agent (Agent.Collector): The agent collecting memory usage data.
        args (tuple, optional): The arguments to pass to the function. Defaults to ().
        kwargs (dict, optional): The keyword arguments to pass to the function. Defaults to {}.

    Returns:
        tuple: The return value of the specified function.
    """
    
    start = time.time()
    memory , retval = mp.memory_usage((func, args, kwargs), max_usage=True, retval=True, include_children=True)
    end = time.time()

    try:
        stored_memory, stored_time = agent.memory_usage[func.__name__] 
        agent.memory_usage[func.__name__] = (stored_memory+memory, stored_time + (end - start))
    except KeyError:
        agent.memory_usage[func.__name__] = (memory, (end - start))
        
    return retval
