import asyncio
import os

import _path  # noqa
from src.avails import const
from src.core import connectserver
from src.managers.statemanager import State
from test import initiate, test_initial_states

if __name__ == "__main__":
    os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    const.debug = False
    i_states = test_initial_states()
    s4 = State("connecting to servers",connectserver.initiate_connection)
    asyncio.run(initiate(i_states + (s4,)), debug=True)
