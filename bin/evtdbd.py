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

__author__ = 'Eric PASCUAL - CSTB (eric.pascual@cstb.fr)'
__copyright__ = 'Copyright (c) 2012 CSTB'
__vcs_id__ = '$Id$'
__version__ = '1.0.0'

import argparse
import sys

import pycstbox.evtdb as evtdb
import pycstbox.cli as cli
import pycstbox.log as log
import pycstbox.dbuslib as dbuslib
import pycstbox.evtdao as evtdao
import pycstbox.evtmgr as evtmgr

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

    args = parser.parse_args()
    loglevel = getattr(log, args.loglevel)

    # defaults an empty channel list and remove duplicates if any
    channels = list(set(args.channels)) if args.channels else [evtmgr.SENSOR_EVENT_CHANNEL]

    dbuslib.dbus_init()

    evtdao.log_setLevel(loglevel)
    daos = [(ch, evtdao.get_dao(DAO_name, ch)) for ch in channels]

    svc = evtdb.EventsDatabase(dbuslib.get_bus(), daos)
    svc.log_setLevel(loglevel)
    try:
        svc.start()
    except Exception as e: #pylint: disable=W
        sys.exit(e)
