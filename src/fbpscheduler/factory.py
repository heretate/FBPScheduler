from __future__ import annotations

from fbpscheduler.enums import ObjectType, DateModifierPolicy, TriggerType, Fields
from fbpscheduler.objects import Job, JobGroup, Process
from fbpscheduler.triggers import CronTrigger, DateTrigger, InstantTrigger
from dateutil.parser import parse



class EntityFactory:
    """
    Class for instantiating Jobs, Job Groups, or Processes.
    """
    @classmethod
    def generate_id(cls, parent_id, object_type, cache):
        prefix = ObjectType.prefix_mapping(object_type)
        parent_node = cache.get_child(parent_id)
        subdelim_str = "-" # Delimiter to separate level type and level number.
        
        new_id = ""
        id_number = 1
       
        if parent_node is not None:
            new_id = new_id + parent_node.id + parent_node.delim_str
            found_id = False
            while not found_id:
                test_id = new_id + prefix + subdelim_str + str(id_number)
                if parent_node.is_child(test_id):
                    id_number = id_number + 1
                else:
                    found_id = True
        new_id = new_id + prefix + subdelim_str + str(id_number)
        return new_id
    
    @staticmethod
    def _parse_job(job_id, job_info):
        new_job = Job(entity_id = job_id, **job_info)
        return new_job
    
    @classmethod
    def _parse_job_group(cls, group_id, job_group_info, cache):
        new_job_group = JobGroup(entity_id = group_id, **job_group_info)    
        
        for job in job_group_info[Fields.jobs]:
                new_job_group.append(cls.parse(group_id, job, cache))
                
        group_jobs = new_job_group.get_entities() 
        ids = {job.name: job.entity_id for job in group_jobs}
    
        for job in group_jobs:
            for name in job.get_dependency_names():
                job.add_dependency(name, ids[name])
            
        return new_job_group

    @classmethod
    def _parse_process(cls, process_id, process_info, cache):
        """ 
        So far, Process is structured essentially as a subclass of Job Group. However, they are purposely inplemented separately with the 
        intention that eventually they may be different. Should this structure be retained as permanent then this component may be 
        reworked.
        """
        new_process = Process(entity_id = process_id, **process_info)
        
        for entity in process_info[Fields.entity_list]:
                new_process.append(cls.parse(process_id, entity, cache))
                
        process_entities = new_process.get_entities() 
        ids = {entity.name: entity.entity_id for entity in process_entities}
    
        for entity in process_entities:
            for name in entity.get_dependency_names():
                entity.add_dependency(name, ids[name])
            
        return new_process
      
    @classmethod
    def parse(cls, parent_id, entity, cache):
        object_type = ObjectType(entity[Fields.object_type])
        
        entity_id = cls.generate_id(parent_id, object_type, cache)
        cache.set_child(entity_id)
        
        if object_type == ObjectType.job:
            parsed_entity = cls._parse_job(entity_id, entity)
        elif object_type == ObjectType.job_group:
            parsed_entity = cls._parse_job_group(entity_id, entity, cache)
        elif object_type == ObjectType.process:
            parsed_entity = cls._parse_process(entity_id, entity, cache)
        else: 
            raise ValueError("Unrecognized object type")
        
        cache.read_state(parsed_entity.get_metadata()) # Initialize the full list of jobs/processes in the cache for the metadata
        return parsed_entity
    
    
class TriggerFactory:
    """
    Class for instantiating instances of the Trigger class.
    """
    
    @staticmethod
    def create_trigger(trigger_info, callback, date_modifier):
        try:
            modifier_action = DateModifierPolicy[trigger_info[Fields.modifier_action]]
        except KeyError:
            # Use default modifier if modifier action is not found - "replace"
            modifier_action = DateModifierPolicy(1)
        
        trigger_type = TriggerType[trigger_info[Fields.trigger_type]]
            
        if trigger_type == TriggerType.cron:
            trigger = CronTrigger(trigger_info[Fields.cron_expression], callback, date_modifier, modifier_action)
        elif trigger_type == TriggerType.datetime:
            trigger = DateTrigger(parse(trigger_info[Fields.trigger_time]), callback, date_modifier, modifier_action)
        elif trigger_type == TriggerType.instant:
            trigger = InstantTrigger(callback)
        else:
            raise ValueError("Unrecognized trigger type " + trigger_type.name)
            
        return trigger
