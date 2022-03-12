from __future__ import annotations

import pandas as pd 
from fbpscheduler.parse import parse_arguments, fill_string, flat_args
from fbpscheduler.cache import Cache
from fbpscheduler.enums import Status, RunType, ExceptionHandlerPolicy
from fbpscheduler.evaluators import python_evaluator, cmd_evaluator
from fbpscheduler.abc import Entity
from fbpscheduler.marshalling import getstate_type_handler, setstate_type_handler
from dataclasses import dataclass

from datetime import datetime
import logging
logger = logging.getLogger(__name__)


def status_handler(func):
    async def wrapper_status_handler(self, cache: Cache, inherited_deadline: datetime = None):
        self._start(inherited_deadline)
        cache.read_state(self.get_metadata())

        status_code = await func(self, cache)
        status_code = self._end(status_code)
        
        cache.read_state(self.get_metadata())
        return status_code
        
    return wrapper_status_handler

def metadata_retriever(func):
    def wrapper_metadata_retriever(self):
        metadata = self.__dict__.copy()
        updates = func(self)
        metadata.update(updates)
        
        return metadata
        
    return wrapper_metadata_retriever

class Graph:
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.graph_entities = {}
        self.graph = pd.DataFrame()
        
    def append(self, entity: Entity):
        self.graph_entities[entity.entity_id] = entity
    
    @staticmethod
    def dictionary_to_graph(dict_graph: dict) -> pd.DataFrame:
        """
        Converts a dictionary to a directed graph stored as a matrix
    
        Parameters
        ----------
        dict_graph : str
            Dictionary used to generate the matrix
    
        Returns
        -------
        DataFrame
            A dataframe that contains the directed graph constructed from the dictionary.
    
        """
        edges = [(source, target) for source, targets in dict_graph.items() for target in targets]
        df = pd.DataFrame(edges)
        if df.empty:
            return df # Return the empty dataframe if no edges
        adj_matrix = pd.crosstab(df[0], df[1]).rename_axis(None).rename_axis(None, axis=1)
        return adj_matrix
        
    def generate_graph(self, apply: bool = True):
        dict_graph = {}
        for key, job in self.graph_entities.items():
            dict_graph.update(job.dependency_to_dict())
        new_graph = self.dictionary_to_graph(dict_graph)
        idx = self.graph_entities.keys()
        
        new_graph = new_graph.reindex(index = idx, columns = idx, fill_value=0)
        if apply:
            self.graph = new_graph
            return None
        else:
            return new_graph
        
    def get_entities(self):
        return self.graph_entities.values()
    
    def get_entity_ids(self):
        return self.graph_entities.keys()

    

@dataclass(init=False)
class Job(Entity):
    
    command: str
    run_type: str
    parameters: dict
    module: str | None = None
    success_code: str | float | None = 0
    parameter_delimiter: str | None = "; "
    exception_handling: ExceptionHandlerPolicy | str = ExceptionHandlerPolicy.kill
    message: str = ""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.run_type = RunType[self.run_type]

    @status_handler
    async def execute(self, cache = None) -> bool:
        params = {}
        if cache is not None:
            params = cache.get_parameters(self.entity_id)
        arguments = parse_arguments(self.parameters, params)
        flat_arguments = flat_args(arguments, self.parameter_delimiter)
        command = fill_string(self.command, params)
                
        if self.run_type == RunType.python:
            module = fill_string(self.module, params)
            self.log("Executing: " + command + " " + flat_arguments + " from " + module)
            self.return_code, logging_info = await python_evaluator(module, command, arguments, cache, self.timeout)
        elif self.run_type == RunType.cmd:
            self.log("Executing: " + command + " " + flat_arguments)
            self.return_code, logging_info = await cmd_evaluator(command, flat_arguments, self.timeout)
        else:
            raise ValueError("Unrecognized run type")
        
        execution_result = (self.return_code == self.success_code)

        if execution_result:
            self.log(logging_info)
        else:
            self.log(logging_info, warning=True)
                
        return 1 - execution_result       

    def log(self, message, warning=False):
        self.message += message
        if self.status != Status.re_running:
            if warning:
                logger.warning(message)
                if self.exception_handling == ExceptionHandlerPolicy.repeat:
                    logger.info("{} will re-run. Future warnings pertaining this job instance will be silenced until deadline has passed.".format(self.name))
            else:
                logger.info(message)

    def terminate(self, cache):
        if self.status != Status.finished:
            self.status = Status.failure
            self.end_time = datetime.now()
            cache.read_state(self.get_metadata())

    @metadata_retriever   
    def get_metadata(self):
        return {}
        
       
    
@dataclass(init=False)
class JobGroup(Graph, Entity):

    exception_handling: ExceptionHandlerPolicy | str = ExceptionHandlerPolicy.repeat
                          
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @status_handler
    async def execute(self, cache: Cache):
        if self.status == Status.running:
            self.generate_graph()
        graph_sum = 1
        execution_status_code = 0

        while graph_sum != 0:
            graph_sum = self.graph.to_numpy().sum()
            for index, row in self.graph.iterrows():
                if (row.sum() == 0) and (self.graph_entities[index].status != Status.finished):
                    child_status_code = await self.graph_entities[index].execute(cache, self.deadline)
                    if child_status_code == 0:
                        self.graph[index] = 0
                    else:
                        execution_status_code = max(execution_status_code, child_status_code)

            if execution_status_code != 0:
                return execution_status_code

        return execution_status_code

    def terminate(self, cache: Cache):
        if self.status != Status.finished:
            self.status = Status.failure
            self.end_time = datetime.now()
            for children in self.graph_entities.values():
                children.terminate(cache)
            cache.read_state(self.get_metadata())

    @metadata_retriever   
    def get_metadata(self):
        updates = {}
        updates["graph_entities"] = [entity_id for entity_id in self.graph_entities.keys()]
        updates['graph'] = self.graph.to_dict()
        return updates
    
    
@dataclass(init=False)
class Process(JobGroup):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    
    @status_handler
    async def execute(self, cache: Cache):
        super().execute(self, cache, self.deadline)

        
            
            
