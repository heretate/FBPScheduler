from __future__ import annotations

import asyncio as aio
from dataclasses import dataclass
from scheduler.abc import Trigger
from datetime import datetime

@dataclass
class ConfigStore:
    config: dict
    last_unmodified: datetime | float
    trigger: Trigger = None
    trigger_task = None

    def set_trigger(self, trigger):
        self.trigger = trigger

    def activate_trigger(self):
        if self.trigger is None:
            print("Trigger not set")
            return None

        self.trigger_task = aio.create_task(self.trigger.activate_trigger())

    async def cancel_trigger(self):
        self.trigger_task.cancel()
        try:
            await self.trigger_task
        except aio.CancelledError:
            print("Trigger Updated")


