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

""" CSTBox Events Database access D-Bus service.

The class defined in this module do not implement the data access layer, but
sits on top of it to provide a generic API for data storage and retrieval.

The data access layer is defined in separate DAO classes, which can be viewed
as drivers for supported persistence mechanism. This approach allows switching
between different underlying storage strategies, without any impact for the
upper layers.

The concrete DAO to be used must be passed to the constructor.
"""

import dbus.exceptions
import dbus.service
import dateutil.parser

from pycstbox.log import Loggable
import pycstbox.evtmgr as evtmgr
import pycstbox.service as service
import pycstbox.dbuslib as dbuslib

__author__ = 'Eric PASCUAL - CSTB (eric.pascual@cstb.fr)'


SERVICE_NAME = "EventDatabase"

ROOT_BUS_NAME = dbuslib.make_bus_name(SERVICE_NAME)
OBJECT_PATH = "/service"
SERVICE_INTERFACE = dbuslib.make_interface_name(SERVICE_NAME)

TIMESTAMP_FMT = '%Y-%m-%d %H:%M:%S.%f'

FILTER_FROM_TIME = 'from_time'
FILTER_TO_TIME = 'to_time'
FILTER_VAR_TYPE = 'var_type'
FILTER_VAR_NAME = 'var_name'


class EventsDatabase(service.ServiceContainer):
    """ CSTBox Event database service.

    This container will host a service object for each event channel, in order
    to keep the various communication separated, and this easing the subscription
    to a given kind of channel.
    """
    def __init__(self, conn, daos):
        """
        :param conn:
            the D-Bus connection (Session, System,...)

        :param daos:
            a list of tuples, containing the channel name and the DAO instance managing
            its events
        """
        if not daos:
            raise ValueError('no DAO provided')

        svc_objects = [
            (EventDatabaseObject(channel, dao), '/' + channel) for channel, dao in daos
        ]

        super(EventsDatabase, self).__init__(SERVICE_NAME, conn, svc_objects)


class EventDatabaseObject(dbus.service.Object, Loggable):
    """ The service object for a given event database.

    One instance of this class is created for managing the persistence of
    each event channel to be managed (see EventDatabase.__init__().
    """
    def __init__(self, channel, dao): #pylint: disable=E1002
        """
        :param str channel: the event channel
        """
        super(EventDatabaseObject, self).__init__()

        self._channel = channel
        self._dao = dao

        Loggable.__init__(self, logname='SO:%s' % self._channel)

    def _event_signal_handler(self, timestamp, var_type, var_name, data):
        self.log_debug(
            "recording event : timestamp=%s var_type=%s var_name=%s data=%s",
            timestamp, var_type, var_name, data)
        self._dao.insert_event(timestamp, var_type, var_name, data)

    def start(self):
        """ Service objet runtime initialization """
        self.log_info('starting svcobj for channel %s', self._channel)
        self._dao.open()

        try:
            svc = evtmgr.get_object(self._channel)
        except dbus.exceptions.DBusException as e:
            self.log_error(e)
            raise RuntimeError('evtmgr service object connection failed')
        else:
            svc.connect_to_signal("onCSTBoxEvent",
                                  self._event_signal_handler,
                                  dbus_interface=evtmgr.SERVICE_INTERFACE)
            self.log_info('connected to EventManager onCSTBoxEvent signal')

    def stop(self):
        """ Cleanup before stop """
        self._dao.close()

    @dbus.service.method(SERVICE_INTERFACE)
    def flush(self):
        """ Flushes pending writes. """
        self._dao.flush()

    @dbus.service.method(SERVICE_INTERFACE, in_signature="nn", out_signature='as')
    def get_available_days(self, year=0, month=0):
        """ Returns the list of days for which events have been stored.

        The search is filtered by the provided year and month if not null.

        Parameters:
            year : int
                the year (0 if no filtering)
            month : int
                the month number (1 <= month <= 12) or 0 if no filtering
                on the month. If year == 0 this parameter is ognored

        The result is an array of dates formatted as "YYYY-MM-DD" strings
        """
        if year:
            time_line_filter = (int(year), int(month))
        else:
            time_line_filter = None
        result = [str(day) for day in self._dao.get_available_days(time_line_filter)]

        return result

    @dbus.service.method(SERVICE_INTERFACE,
                         in_signature='sss',
                         out_signature='a(sssva{sv})')
    def get_events_for_day(self, day, var_type=None, var_name=None):
        """ Returns the list of events matching the provided criteria.

        The result is an array of tuples representing the properties of the
        events (timestamp, var_type, var_name, value, dictionary of additional
        infos).

        Events are returned in D-Bus compatible format

        :param str day:
            the date of the requested days (as a valid SQL date)
        :param str var_type:
            (optional) an event class ('temperature',...) for filtering the returned list
        :param str var_name:
            (optional) the var_name of events for filtering the returned list

        :returns: a list of events, as serializable tuples
        """
        self.log_debug("get_events_for_day('%s','%s','%s') called" %
                           (day, var_type, var_name))

        return [(
                    evt.timestamp.strftime(TIMESTAMP_FMT),
                    evt.var_type,
                    evt.var_name,
                    evt.value,
                    evt.data
                )
                for evt in self._dao.get_events_for_day(day, var_type, var_name)]

    @dbus.service.method(SERVICE_INTERFACE,
                         in_signature='a{sv}',
                         out_signature='a(sssva{sv})')
    def get_events(self, event_filter):
        """ Returns the list of events matching the provided filter.

        Events are returned in D-Bus compatible format

        :param dict event_filter:
            DAOs get_events() method keyword parameters as a dictionary

        :returns: a list of events, as serializable tuples
        """
        self.log_debug("get_events(%s) called", event_filter)

        if FILTER_FROM_TIME in event_filter:
            from_time = dateutil.parser.parse(event_filter[FILTER_FROM_TIME])
            from_day = from_time.date()
        else:
            from_time = from_day = None

        if FILTER_TO_TIME in event_filter:
            to_time = dateutil.parser.parse(event_filter[FILTER_TO_TIME])
            to_day = to_time.date()
        else:
            to_time = to_day = None

        return [(
                    evt.timestamp.strftime(TIMESTAMP_FMT),
                    evt.var_type,
                    evt.var_name,
                    evt.value,
                    evt.data
                )
                for evt in
                self._dao.get_events(
                    from_time=from_time,
                    to_time=to_time,
                    var_type=event_filter.get(FILTER_VAR_TYPE, None),
                    var_name=event_filter.get(FILTER_VAR_NAME, None)
                )]


def get_object(channel):
    """Returns the service proxy object for a given event channel if available

    :param str channel: the event channel managed by the requested service instance
    :returns: the requested service instance, if exists
    :raises ValueError: if no bus name match the requested channel
    """
    return dbuslib.get_object(SERVICE_NAME, '/' + channel)
