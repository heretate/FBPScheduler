from scheduler.abc import Trigger
from croniter import croniter
from datetime import datetime
from dateutil import parser


class CronTrigger(Trigger):
    """
    Trigger type object that operates based on cron expressions.
    
    i.e. */2 * * * * indicates a trigger that hits every two minutes
         0 8 * * * indicates a trigger that hits daily at 8 am local time
         - Time zone is currently based on machine time zone. Support for time zone specification may be included in the future
         
    """
    def __init__(self, cron_expression, callback, date_modifier, modifier_action):
        super().__init__(callback, date_modifier, modifier_action)
        if croniter.is_valid(cron_expression):
            self._cron_exp = cron_expression
            self._iter = croniter(self._cron_exp, datetime.now())
            self._trigger_date = self._iter.get_next(datetime)
        else:
            print("Cron expression: {} is not valid".format(cron_expression))
            self._trigger_date = None

    def next(self):
        next_date = self._iter.get_next(datetime)
        return next_date


class DateTrigger(Trigger):
    """
    Trigger type object that triggers once at the specified datetime
    """
    def __init__(self, trigger_date, callback, date_modifier, modifier_action):
        super().__init__(callback, date_modifier, modifier_action)
        self._trigger_date = parser.parse(trigger_date)

    def next(self):
        return None


class InstantTrigger(Trigger):
    def __init__(self, callback):
        super().__init__(callback=callback, date_modifier=None, modifier_action=None)
        self._trigger_date = datetime.now()

    def next(self):
        return None

        

