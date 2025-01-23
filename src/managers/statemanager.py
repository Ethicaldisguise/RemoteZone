import asyncio
import functools
import inspect
import math
import threading
from asyncio import Queue as _queue
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor
from typing import Iterable, Optional

from src.avails import useables
from src.avails.useables import echo_print


def _get_func_name(func):
    """Retrieve the name of a function or callable in a robust way."""
    # Handle partial functions
    if isinstance(func, functools.partial):
        return func.func.__name__

    # Handle bound and unbound methods
    if inspect.ismethod(func):
        return func.__func__.__name__

    # Handle regular functions and callables
    if hasattr(func, "__name__"):
        return func.__name__

    # Fallback to frame inspection if no name is found
    if frame := inspect.currentframe():
        return frame.f_code.co_name

    # Default name if everything else fails
    return "not defined"


class State:
    def __init__(self, name, func, is_blocking=False, controller=None, event_to_wait: asyncio.Event = None):
        self.name = name

        @functools.wraps(func)
        async def wrap_in_task(_func):
            f = useables.wrap_with_tryexcept(_func)
            return asyncio.create_task(f())

        @functools.wraps(func)
        def wrap_in_thread(_func):
            threading.Thread(target=func, daemon=True).start()

        self.is_coro = inspect.iscoroutinefunction(func)

        if is_blocking:
            self.func = functools.partial(wrap_in_task if self.is_coro else wrap_in_thread, func)
        else:
            self.func = func

        self.func_name = _get_func_name(func)

        self.actuator = controller
        self.event = event_to_wait

    async def enter_state(self):
        loop = asyncio.get_event_loop()
        x = loop.time()
        echo_print(f"[{x - math.floor(x):.5f}s] CORO:{self.is_coro} entering state :", self.name, end=' ')
        print("func:", self.func_name)

        if self.event:
            await self.event.wait()

        if self.is_coro:
            ret_val = await self.func()
        else:
            ret_val = self.func()

        return ret_val


class StateManager:
    """
    A Singleton class
    Sort of task queue
    process_states is called at the begining of program
    """
    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(StateManager, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self.state_queue = _queue()

        self.executor = _ThreadPoolExecutor()
        self.close = False
        self._initialized = True
        self.last_state = None
        self.global_wait = threading.Event()
        self.all_tasks: list[asyncio.Task] = []

    def signal_stopping(self):
        self.close = True
        self.state_queue.put(None)
        for t in self.all_tasks:
            t.cancel("finalizing from state manager")

    async def put_state(self, state: Optional[State]):
        await self.state_queue.put(state)

    async def put_states(self, states: Iterable[State]):
        for state in states:
            await self.state_queue.put(state)

    async def process_states(self):
        """
        Main event loop for the program
        different points in code wrap functions in states to get processed
        """
        while self.close is False:
            try:
                current_state: State = await self.state_queue.get()
                r = await current_state.enter_state()
                if isinstance(r, asyncio.Task):
                    self.all_tasks.append(r)

            except KeyboardInterrupt:
                print('received keyboard interrupt finalizing')
                exit(1)


END_STATE = State('Final State', func=lambda: None)
