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

""" Base material for DAOs implementation and usage.
"""
__author__ = 'Eric PASCUAL - CSTB (eric.pascual@cstb.fr)'
__copyright__ = 'Copyright (c) 2013 CSTB'
__vcs_id__ = '$Id$'
__version__ = '1.0.0'

from datetime import datetime
import os.path

import importlib
from collections import namedtuple
from copy import deepcopy

import pycstbox.evtmgr as evtmgr
import pycstbox.log as log
from pycstbox.config import GlobalSettings

log.setup_logging()
_logger = log.getLogger('evtdao')


def log_setLevel(level):
    _logger.setLevel(level)

DriverSpecs = namedtuple('DriverSpecs', 'modname cfg')

CFGKEY_EVTS_DB_HOME_DIR = 'evts_db_home_dir'

#
# The dictionary of the supported DAOs, together with their configuration
# parameters
#
# TODO replace static dictionary by a discovery mechanism
#
_known_DAOs = {
#    'sqlite': DriverSpecs(
#        modname = 'pycstbox.evtdao.sqlite.dao_apsw',
#        cfg = {'dbhome' : '/var/db/cstbox'}
#    ),
    'fsys': DriverSpecs(
        modname='pycstbox.evtdao.fsys.dao_fsys',
        cfg={CFGKEY_EVTS_DB_HOME_DIR : '%(db_home_dir)s/events'}
    )
}


def get_dao(dao_name, events_channel=evtmgr.SENSOR_EVENT_CHANNEL, config=None):
    """ Returns an instance of the DAO specified by its name, and for a given
    event channel.

    :param str dao_name:
            the name of the requested DAO (must be defined in the _known_DAOs
            dictionnary above)
    :param str events_channel:
            the channel of the events (default: sensor events)
    :param dict config:
            optional dictionary containing configuration parameters overriding
            the default ones, or defining additional substitution variables

    :returns: the DAO instance

    :raises ImportError: if we cannot import the module containing the DAO class
    :raises KeyError: if the given DAO name dos not exist
    """
    _dao_specs = _known_DAOs[dao_name]
    try:
        dao_module = importlib.import_module(_dao_specs.modname)
    except ImportError:
        _logger.error("failed to import DAO module '%s'", _dao_specs.modname)
        raise
    else:
        log.debug("using %s as DAO", dao_name)

        # build the effective configuration parameters by taking DAO default
        # ones and override them with passed ones if any
        gs = GlobalSettings().as_dict()
        if config:
            cfg = deepcopy(_dao_specs.cfg)
            cfg.update(config)
            # add config paramter provided variables to global settings ones
            gs.update(config)
        else:
            cfg = _dao_specs.cfg

        # substitute variables (if any) in configuration values, handling
        # replacement of home dir and environment variables
        for key in cfg:
            value = cfg[key] % gs
            cfg[key] = os.path.expanduser(os.path.expandvars(value))

        return dao_module.EventsDAO(
            events_channel=events_channel,
            config=cfg
        )

FILTER_FROM_TIME = 'from_time'
FILTER_TO_TIME = 'to_time'
FILTER_VAR_TYPE = 'var_type'
FILTER_VAR_NAME = 'var_name'

DATE_FMT = '%Y-%m-%d'
TOD_FMT = '%H:%M:%S'
TS_FMT_SECS = DATE_FMT + ' ' + TOD_FMT
TS_FMT_FULL = TS_FMT_SECS + '.%f'


class AbstractDAO(log.Loggable):
    """ Root abstract class for implementing a DAO.

    It defines the mandatory methods by making them raise a NotImplementedError,
    and optional ones by providing an empty content, so that there is not need
    to code them in every concrete implementation not requiring them.
    """

    def __init__(self, events_channel):
        """
        :param str events_channel:
                the name of the events channel to which this DAO is associated.
                Refer to pycstbox.evtmgr module for events channels topic and
                pre-defined ones.
        """
        log.Loggable.__init__(self, logname='EventsDAO(ch:%s)' % events_channel)

    def insert_event(self, msecs, var_type, var_name, data):
        """ Inserts an event in the database.

        This method is mandatory.

        :param long msecs:
                the event timestamp expressed in milliseconds from time origin (Jan 1st 1970)
        :param str var_type: the variable type
        :param str var_name: the variable name
        :param str_or_dict data: the event data as a dict or as its valid JSON representation
        """
        raise NotImplementedError()

    def insert_timed_event(self, event):
        """ Convenience method handling a instance of pycstbox.events.TimedEvent
        for the event to be inserted in the database.
        """
        self.insert_event(
            event.timestamp,
            event.var_type,
            event.var_name,
            event.data
        )

    def get_available_days(self, month=None):
        """ Generator returning  the list of days for which we have events in
        the database

        This method is mandatory.

        Parameters:
            month : tuple (optional)
                a tuple containing the year and month number (1 <= month <= 12)
                if only days for this month are wanted
        Result:
            the matching days (as datetime.date instances)
        """
        raise NotImplementedError()

    def get_events_for_day(self, day, var_type=None, var_name=None):
        """ Generator returning the events available for a given day,
        optionaly filtering them by event class and/or var_name.

        This method is mandatory.

        :param str_or_date day:
                the day for which the events must be extracted.
                If provided as a string, it must be formated YYYY-MM-DD
                (following chars are ignored)
        :param str var_type:
                an optional variable type (eg: temperature) which is used to
                filter the extracted events if provided

        :returns: the list of corresponding events (as pycstbox.events.TimedEvent instances),
        if any
        """
        raise NotImplementedError()

    def get_events(self, event_filter):
        """ Generator for general event queries.

        The events are filtered based on the criteria defined by the
        event_filter parameter. It is a dictionary which keys are the following
        predefined criteria :

        - from_time : timestamp (in SQL format) of the first event to be returned
        - to_time : same for the timestamp of the last event
        - var_type : type of the variable
        - var_name : name of the variable

        Beware that, depending on the filter provided, the amount of returned
        events can be consequent.

        :param event_filter: request criteria

        :returns: the list of corresponding events (as pycstbox.events.TimedEvent instances),
        if any
        """
        pass

    def open(self):
        """ Opens the database, creating it on the fly if not yet available.

        This is an optional method, depending on the underlying implementation.

        Calling open() for an already opened database should do nothing,
        apart maybe loging it as a warning.
        """
        pass

    def close(self):
        """ Closes the database if not yet done

        This is an optional method, depending on the underlying implementation.
        """
        pass

    def __enter__(self):
        """ Context entry.

        Sub-classes MUST override this method and return the context which
        is affected to the "with as" variable
        """
        raise NotImplementedError()

    def __exit__(self, type_, value, traceback):
        pass


def strptime(s):
    """ Friendly SQL timestamp parsing function.

    It accepts partial forms and silently defaults missing parts
    """
    return datetime.strptime(
        s + '0000-00-00 00:00:00.000'[len(s):],
        TS_FMT_FULL
    )

