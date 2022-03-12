from fbpscheduler.abc import Node
from fbpscheduler.enums import Fields
from fbpscheduler.parse import flat_args
import json

import logging
logger = logging.getLogger(__name__)


def update_parent_cache(cache, child_id, new_parameters: dict = {}):
    split_id = cache.split_id(child_id)
    split_id.pop()
    # Parent is assumed to be the process
    parent_id = cache.delim_str.join(split_id)
    cache.update_parameters(parent_id, new_parameters)
    logger.info("Parameters for {id} updated. New parameters: {param_list}".format(id=parent_id, param_list=flat_args(new_parameters, delimiter=" ")))


def update_process_cache(cache, entity_id, new_parameters: dict = {}):
    split_id = cache.split_id(entity_id)
    process_id = cache.delim_str.join([split_id[0], split_id[1]])
    cache.update_parameters(process_id, new_parameters)
    logger.info("Parameters for {id} updated. New parameters: {param_list}".format(id=process_id, param_list=flat_args(new_parameters, delimiter=" ")))


class Cache(Node):
    def __init__(self, cache_id: str, parameters: dict = {}, cache_handler = None, entity_handler = None):
        super().__init__(cache_id)
        self.parameters = {self.id: parameters}
        self.metadata = {}
        self.cache_handler = cache_handler
        self.entity_handler = entity_handler

    def set_parameters(self, node_id, parameters: dict = {}):
        self.parameters[node_id] = parameters
    
    def update_parameters(self, node_id, parameters):
        self.parameters[node_id].update(parameters)
    
    def get_parameters(self, node_id, look_back = True):
        output_parameters = {}
        output_parameters[Fields.entity_id.value] = node_id
        
        lookup_list = [node_id]
        if look_back:
            segmented_id = self.split_id(node_id)
            segmented_id.pop()
            while segmented_id != []:
                new_id = type(self).delim_str.join(segmented_id)
                lookup_list.append(new_id)
                segmented_id.pop()
            
        while lookup_list != []:
            param = self.parameters[lookup_list.pop()]
            output_parameters.update(param)
        
        return output_parameters

    def set_metadata(self, metadata):
        self.metadata[metadata[Fields.entity_id.name]] = metadata
        
    def get_metadata(self, node_id=None):
        if node_id is not None:
            return self.metadata[node_id]
        return self.metadata

    def read_state(self, metadata, run_handlers = True):
        self.set_metadata(metadata)
        if run_handlers:
            if self.cache_handler is not None:
                self.cache_handler(self.__dict__.copy())
            if self.entity_handler is not None:
                self.entity_handler(metadata, self.get_parameters(metadata[Fields.entity_id.name]))

    
    def set_child(self, node_id):
        splitnode_id = self.split_id(node_id)
        splitnode_id.pop() # List is now the split parent Id
        if len(splitnode_id) == 1:
            self._children[node_id] = CacheNode(node_id)
            self._children[node_id].parent = self
        else:
            target_node = self.get_child(type(self).delim_str.join(splitnode_id))
            target_node.set_child(node_id)
        self.set_parameters(node_id)
        
    
    

class CacheNode(Node):
    def __init__(self, node_id):
        super().__init__(node_id)
        self._parent = None
    
    @property
    def parent(self):
        return self._parent
    
    @parent.setter
    def parent(self, value):
        self._parent = value
    

    def set_child(self, node_id):
       if (self.id in node_id):
           child = CacheNode(node_id)
           child._parent = self
           self._children[node_id] = child  
            
        
    
