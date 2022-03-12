from __future__ import annotations

from collections.abc import Callable
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, fields, field

from datetime import datetime, date
from fbpscheduler.evaluators import python_evaluator
from fbpscheduler.enums import ObjectType, Status, DateModifierPolicy, ExceptionHandlerPolicy, Fields as SchedulerFields
from fbpscheduler.marshalling import getstate_type_handler, setstate_type_handler
import asyncio as aio

class Scheduler(metaclass=ABCMeta):

    def __init__(self, entity_id: str):
        self.id = entity_id



@dataclass(init=False)
class Entity(metaclass=ABCMeta):
    
    name: str
    entity_id: str
    object_type: str
    description: str = ""
    dependencies: list(str) = field(default_factory=list)
    start_time: None = None
    end_time: None = None
    timeout: None = None
    deadline: str = None
    status: Status = Status.initialized
    

    def __init__(self, **kwargs):
        string_names, variable_names = map(set, zip(*[[SchedulerFields[f.name].value, f.name] for f in fields(self)]))
        for k, v in kwargs.items():
            if k in string_names:
                setattr(self, SchedulerFields(k).name, v)
                variable_names.remove(SchedulerFields(k).name)
            elif k in variable_names:
                setattr(self, k, v)
                variable_names.remove(k)
            else:
                continue
        
        # There is issues with attribute initialization through dataclass with init=False. 
        # Foo.__dict__ does not contain attributes that has not been referenced at least once explicitly
        for k in variable_names:
            setattr(self, k, getattr(self, k))
        
        self.dependencies = dict.fromkeys(self.dependencies)
        self.object_type = ObjectType(self.object_type)
        if type(self.exception_handling) == str:
            self.exception_handling = ExceptionHandlerPolicy[self.exception_handling]


        # For cooperative inheritance, cannot pass parameters to subclass, so it is recommended to
        #  position Entity as the last element before object with respect to MRO.
        super().__init__()

    def __eq__(self, other):
        if isinstance(other, Entity):
            return self.entity_id == other.entity_id
    
        return False
            
    def evaluate_condition(self, command, cache = None):
        module, function = command[0], command[1]
        params = {}
        if cache is not None:
            params = cache.get_parameters(self.entity_id)
        return python_evaluator(module, function, params, cache)
        
    def _start(self, inherited_deadline: datetime = None):
        if self.status == Status.initialized:
            self.start_time = datetime.now()
            if self.deadline:
                timeout = datetime.combine(date.min, datetime.strptime(self.deadline, '%H:%M:%S').time()) - datetime.min
                self.deadline = min(datetime.now() + timeout, inherited_deadline)
            else:
                self.deadline = inherited_deadline
            self.timeout = self.deadline - datetime.now()
            self.status = Status.running
        elif self.status == Status.unsuccessful:
            self.status = Status.re_running
        else:
            raise NameError("Invalid status for start of execution")
        
        self.timeout = self.deadline - datetime.now()
        
    def _end(self, execution_status_code):

        if execution_status_code == Status.finished.value:
            self.status = Status.finished
        else:
            # Can use case switch in python version 3.10
            if self.exception_handling == ExceptionHandlerPolicy.kill:
                self.status = Status.failure
            elif self.exception_handling == ExceptionHandlerPolicy.repeat:
                self.status = Status(execution_status_code)
            elif self.exception_handling == ExceptionHandlerPolicy.skip:
                self.status = Status.finished
            else:
                raise NameError("Invalid status for end of execution")

        if self.status != Status.unsuccessful:
            self.end_time = datetime.now()
            print("{name} ended. Status code: {code}".format(name=self.name, code=self.status.value))

        return self.status.value
        
    def get_dependency_names(self) -> list(str):
        return self.dependencies.keys()
    
    def get_dependency_ids(self) -> list(str):
        return self.dependencies.values()
    
    def add_dependency(self, dependency_name: str, dependency_id: str):
        """ 
        Extends current dependency list with new entries. entity Ids must be an iterable
        or a scalar.
        
        Raises type error when extending on a non-iterable, after which the 
        function tries to append instead.
        """
        self.dependencies[dependency_name] = dependency_id
    
    def dependency_to_dict(self):
        return {self.entity_id: self.dependencies.values()}
    
    @abstractmethod
    async def execute(self):
        pass

    @abstractmethod
    def terminate(self):
        pass

    @abstractmethod
    def get_metadata(self):
        pass
    

class Trigger(metaclass=ABCMeta):
    def __init__(self, callback: Callable, date_modifier: Callable | None, modifier_action: str | None):
        self._trigger_date = None
        self._modifier_action = modifier_action
        self._date_modifier = date_modifier
        self._callback = callback

    async def activate_trigger(self):
        while self._trigger_date is not None:
            if self._date_modifier is not None:
                new_date = self._date_modifier(self._trigger_date)
                self._apply_modification(new_date)

            sleep_time = max(int((self._trigger_date - datetime.now()).total_seconds()), 0)
            await aio.sleep(sleep_time)
            self._callback()
            self._trigger_date = self.next()
        else:
            print("Trigger will no longer call back.")

        
    def _apply_modification(self, new_date: datetime) -> None:
        if new_date != self._trigger_date:
            if self._modifier_action == DateModifierPolicy.keep:
                self._trigger_date = new_date

            elif self._modifier_action == DateModifierPolicy.delete:
                self._trigger_date = self.next()
            elif self._modifier_action == DateModifierPolicy.unmodify:
                return None
            else:
                raise ValueError("Unrecognized modifier action " + self._modifier_action.name)  
        else:
            return None

    @abstractmethod
    def next(self):
        pass


class Node(metaclass = ABCMeta):
    
    # Static field
    delim_str = "."

    def __init__(self, node_id):
        self.id = node_id
        self._children = {}
        
    @staticmethod
    def split_id(node_id, delimiter = delim_str):
        return node_id.split(delimiter)
    
    def is_child(self, node_id):
        if node_id in self._children.keys():
            return True
        else:
            return False
        
    def get_child(self, node_id):
        if node_id == self.id:
            return self
        else:
            # Since ids are unique, this recursive call should return once
            target_child = None
            for child_id, child in self._children.items():
                if child_id in node_id:
                    retrieved_child =  child.get_child(node_id)
                    if retrieved_child is not None:
                        target_child = retrieved_child
                        
            return target_child
        
        print("Unable to find the node " + node_id)
        return None
    
    @abstractmethod
    def set_child(self, node_id):
        pass
    
