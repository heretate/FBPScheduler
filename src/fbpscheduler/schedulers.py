# -*- coding: utf-8 -*-
"""
Created on Mon May 10 10:23:28 2021

@author: ehuan
"""

from __future__ import annotations

from os import listdir
from os.path import isfile, join, getmtime

from datetime import datetime
import asyncio as aio
from collections.abc import Callable
from functools import partial

from scheduler import schema_dir
from scheduler.abc import Scheduler
from scheduler.cache import Cache
from scheduler.factory import EntityFactory, TriggerFactory
from json import load as json_load, dump as json_dump, JSONDecodeError
from scheduler.marshalling import validate_json
from scheduler.enums import Fields, Status
from scheduler.configstore import ConfigStore
from uuid import uuid4

from pickle import dump, load

import logging




class LocalScheduler(Scheduler):

    def __init__(self, read_path, save_path=None, date_modifier=None,
                 termination_handler=None, cache_handler=None, entity_handler=None,
                 session_parameters=None, logger=None):
        sch_id = "S-{date}".format(date=datetime.now().strftime("%Y%m%d%H%M%S"))
        super().__init__(sch_id)
        self.read_path = read_path
        self.save_path = save_path
        self.cache = Cache(self.id, session_parameters, cache_handler, entity_handler)

        self.logger = logger
        if not self.logger:
            self.logger = logging.getLogger(__name__)
            self.logger.setLevel(logging.DEBUG)

        with open(join(schema_dir, "ProcessSchema.json")) as f:
            self.process_schema = json_load(f)
            self.schema_path = "file:/{0}/".format(schema_dir).replace("\\", "/")

        self.process_configs = {}
        self.date_modifier = date_modifier
        self.termination_handler = termination_handler

        self.initiated_processes = []
        self.run_queue = []
        self.ended_processes = []

    def __getstate__(self):
        attr_dict = self.__dict__.copy()
        serialized_dict = {}
        for key, value in attr_dict["process_configs"].items():
            serialized_dict[key] = ConfigStore(config=value.config,
                                               last_unmodified=value.last_unmodified,
                                               trigger=value.trigger)
        attr_dict["process_configs"] = serialized_dict
        return attr_dict

    def __setstate__(self, state):
        self.__dict__ = state

    def _check_insert(self, file_name):
        if file_name not in self.process_configs:
            return True
        elif getmtime(join(self.read_path, file_name)) != self.process_configs[file_name].last_unmodified:
            return True
        else:
            return False

    async def _file_check(self):
        process_file_names = [f for f in listdir(self.read_path) if isfile(join(self.read_path, f))]
        for file_name in process_file_names:
            if self._check_insert(file_name):
                try:
                    with open(join(self.read_path, file_name)) as g:
                        process_json = json_load(g)
                except JSONDecodeError:
                    self.logger.warning("Invalid JSON file for %s. Json could not be decoded.", file_name)
                    continue
                except PermissionError:
                    self.logger.warning("Could not access %s. Will try again later.", file_name)
                    continue

                if validate_json(process_json, self.process_schema, self.schema_path):
                    if file_name in self.process_configs.keys():
                        await self.process_configs[file_name].cancel_trigger()
                    callback = partial(self.trigger_callback, process_json=process_json)
                    trigger = TriggerFactory.create_trigger(process_json[Fields.trigger], callback, self.date_modifier)
                    self.process_configs[file_name] = ConfigStore(config=process_json,
                                                                  last_unmodified=getmtime(join(self.read_path, file_name)),
                                                                  trigger=trigger)
                    self.process_configs[file_name].activate_trigger()

                    self.logger.info("Inserted process %s", file_name)

                else:
                    self.logger.warning("Invalid configuration for %s", file_name)
                    continue

            try:
                error = self.process_configs[file_name].trigger_task.exception()
                if error is not None:
                    self.logger.critical("Exception raised: %s", str(error))
            except aio.exceptions.InvalidStateError:
                pass


    def trigger_callback(self, process_json):
        process = EntityFactory.parse(self.id, process_json, self.cache)
        self.logger.info("{Name} has been triggered. Process {Id} has been generated".format(Name=process.name,
                                                                                  Id=process.entity_id))
        self.initiated_processes.append(process)

    async def _condition_check(self):
        for process in self.initiated_processes:
            if process.check_conditions():
                #print(process.entity_id + " passed conditions check")
                self.run_queue.append(process)
        self.initiated_processes = list(filter(lambda x: x not in self.run_queue, self.initiated_processes))

    def _terminate_process(self, process):
        process.terminate(self.cache)
        self.ended_processes.append(process)
        self.run_queue.remove(process)
        if self.termination_handler is not None:
                self.termination_handler(process)


    async def _execute_process(self, process):
        self.save_state()
        await process.execute(self.cache)

        if process.status in [Status.finished, Status.failure]:
            self._terminate_process(process)
 
        return None

    async def _execute(self):
        for process in self.run_queue:
            if process.deadline <= datetime.now():
                self.logger.warning("Process {} exceeded specified deadline".format(process.entity_id))
                self._terminate_process(process)
            else:
                if process.status not in [Status.running, Status.re_running]:
                    if process.status != Status.unsuccessful:
                        self.logger.info("Executing %s", process.entity_id)
                    else:
                        await aio.sleep(60)
                    aio.create_task(self._execute_process(process))

    async def _start_loop(self):
        while True:
            await self._file_check()
            await self._condition_check()
            await self._execute()
            await aio.sleep(3)

    def run(self):
        loop = aio.get_event_loop()
        aio.ensure_future(self._start_loop())
        if not loop.is_running():
            loop.run_forever()

    def save_state(self):
        if self.save_path is not None:
            with open(join(self.save_path, self.id + ".pkl"), "wb") as f:
                dump(self, f)

    @classmethod
    def load_state(cls, path):
        with open(path, "rb") as f:
            scheduler = load(f)
        return scheduler

    def set_date_modifier(self, modifier: Callable[[datetime], datetime]):
        if callable(modifier):
            self.date_modifier = modifier
        else:
            self.logger.warning("Invalid date modifier function")

    def set_termination_handler(self, handler: Callable):
        if callable(handler):
            self.termination_handler = handler
        else:
            self.logger.warning("Invalid termination handler function")
