import asyncio
from typing import override

from src.core import Dock


class AnotherRunner(asyncio.Runner):  # noqa # dirty dirty dirty
    @override
    def _on_sigint(self, signum, frame, main_task):
        Dock.finalizing.set()
        return super()._on_sigint(signum, frame, main_task)


asyncio.Runner = AnotherRunner
