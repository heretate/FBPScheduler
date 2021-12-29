from enum import Enum, auto


class Fields(str, Enum):
    object_type = "Object Type"

    name = "Name"

    description = "Description"

    deadline = "Deadline"

    exception_handling = "Exception Handling"

    conditions = "Conditions"

    dependencies = "Dependencies"

    start_time = "Start Time"

    end_time = "End Time"

    status = "Status"
    # Job Field Enums

    run_type = "Run Type"

    command = "Command"

    parameters = "Parameters"

    module = "Module"

    parameter_delimiter = "Parameter Delimiter"

    success_code = "Success Code"

    return_code = "Return Code"

    message = "Message"

    # Job Group Field Enums

    jobs = "Jobs"

    # Process Field Enums

    trigger = "Trigger"

    entity_list = "Entity List"

    # Trigger Field Enums

    trigger_type = "Trigger Type"

    trigger_time = "Trigger Time"

    modifier_action = "Modifier Action"

    cron_expression = "Cron Expression"

    # Scheduler Field Enums

    last_unmodified = "Last Unmodified"

    json = "Json"

    trigger_task = "Trigger Task"

    # Cache Field Enums

    entity_id = "Entity Id"

    cache = "Cache"


class Status(Enum):
    # Object has not been run 
    initialized = -3

    # Object is currently running
    running = -2

    # Object is currently re-running
    re_running = -1

    # Object has successfully finished running
    finished = 0

    # Object did not run successfully. Will re-run in next iteration
    unsuccessful = 1

    # Object has failed
    failure = 2


class RunType(Enum):
    # Executes a python function in some specified module
    python = auto()

    # Executes a command line expression
    cmd = auto()


class ExceptionHandlerPolicy(Enum):
    kill = auto()

    repeat = auto()

    skip = auto()


class DateModifierPolicy(Enum):
    # If new date is different from current date, replace trigger date with new date
    keep = auto()

    # Do not replace current trigger date, regardless of new date
    unmodify = auto()

    # If new date is different from current date, delete the current trigger date and go to next one if applicable
    delete = auto()


class ObjectType(str, Enum):
    # Represents the Job class
    job = "Job"

    # Represents the JobGroup class
    job_group = "JobGroup"

    # Represents the Process class
    process = "Process"

    # Represents the Scheduler class
    scheduler = "Scheduler"


    @classmethod
    def prefix_mapping(cls, object_name):
        if object_name == cls.job:
            return "J"
        elif object_name == cls.job_group:
            return "JG"
        elif object_name == cls.process:
            return "P"
        elif object_name == cls.scheduler:
            return "S"
        else:
            return None


class TriggerType(Enum):
    # Trigger operates based on cron expression
    cron = auto()

    # Trigger is set for specified date
    datetime = auto()

    # Trigger executes immediately
    instant = auto()
