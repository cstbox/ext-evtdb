#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This file is part of CSTBox.
#
# CSTBox is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# CSTBox is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with CSTBox.  If not, see <http://www.gnu.org/licenses/>.

""" Event database manager service daemon.

Starts the service objects handling the access via D-Bus to the database(s)
associated to one or more events channel.
"""

import argparse
import sys

from pycstbox import evtdb 
from pycstbox import cli 
from pycstbox import log 
from pycstbox import dbuslib 
from pycstbox import evtdao 
from pycstbox import evtmgr 

__author__ = 'Eric PASCUAL - CSTB (eric.pascual@cstb.fr)'

# the database "driver" is selected here
DAO_name = 'fsys'


def _event_channel_name(s):
    channel = s.lower()
    if channel in [evtmgr.SENSOR_EVENT_CHANNEL,
                   evtmgr.SYSMON_EVENT_CHANNEL,
                   evtmgr.FRAMEWORK_EVENT_CHANNEL]:
        return channel
    else:
        raise argparse.ArgumentTypeError('invalid channel name : %s' % s)

if __name__ == '__main__':
    parser = cli.get_argument_parser(description="CSTBox Event Database service")
    parser.add_argument(
        'channels',
        nargs='*',
        help='Event channels list. If none specified, defaulted to sensor events channel',
        type=_event_channel_name
    )
    parser.add_argument(
        '--flash_memory',
        help="optimize IO strategy for flash memory",
        dest='flash_memory',
        action='store_true',
        default=False
    )

    args = parser.parse_args()
    loglevel = getattr(log, args.loglevel)

    # use a minimal channel list if not supplied, and remove duplicates if any
    channels = list(set(args.channels)) if args.channels else [evtmgr.SENSOR_EVENT_CHANNEL]

    dbuslib.dbus_init()

    evtdao.log_setLevel(loglevel)
    config = {
        evtdao.CFGKEY_FLASH_MEM_SUPPORT: args.flash_memory
    }
    daos = [(ch, evtdao.get_dao(DAO_name, ch, config=config)) for ch in channels]

    svc = evtdb.EventsDatabase(dbuslib.get_bus(), daos)
    svc.log_setLevel(loglevel)
    try:
        svc.start()
    except Exception as e: #pylint: disable=W
        sys.exit(e)
