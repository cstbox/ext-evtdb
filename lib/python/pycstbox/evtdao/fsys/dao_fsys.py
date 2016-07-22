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

""" File based storage for events.
"""

import os
from datetime import date, datetime
import json
import time

from pycstbox import evtdao
from pycstbox import evtmgr
from pycstbox import events

__author__ = 'Eric PASCUAL - CSTB (eric.pascual@cstb.fr)'


_FNAME_DATE_FMT = '%y%m%d'
_FILE_EXT = '.evt-log'
_FLD_SEP = '\t'
_TS_FMT = '%y%m%d-%H%M%S.%f'


class EventsDAO(evtdao.AbstractDAO):
    """ Implements the event data object as a file based storage.

    The events are stored in plain tabulated text files, using a distinct file
    for each day, named using the pattern "YYMMDD.evt-log".
    """
    MAX_FLUSH_AGE = 3600 * 2        # flush files every 2 hours at least

    class Error(Exception):
        """ Exceptions specialized for this DAO."""
        pass

    def __init__(self,
                 events_channel=evtmgr.SENSOR_EVENT_CHANNEL,
                 config=None,
                 readonly=False):
        """ Constructor.

        Files containing the events are stored in the directory which path is
        defined by <dbhome>/<channel>. If the database is opened in write mode,
        the storage directories are created if not yet available.

        Parameters:
            events_channel:
                the channel (ie sensor, sysmon,...) of the events to be stored
            config:
                the DAO configuration parameters (mandatory)
            readonly:
                guess what... (default: False)

        Raises:
            ValueError:
                if mandatory parameters not provided
            IOError:
                if provided dbhome does not exists, is not a directory or
                cannot be written to
        """

        if not config:
            raise ValueError("missing mandatory parameter : config")

        dbhome = config[evtdao.CFGKEY_EVTS_DB_HOME_DIR]
        if not os.path.exists(dbhome):
            if readonly:
                raise IOError('path not found : %s' % dbhome)
            else:
                os.mkdir(dbhome)
        else:
            if not os.path.isdir(dbhome):
                raise IOError('path is not a directory : %s' % dbhome)

        super(EventsDAO, self).__init__(events_channel)

        self._dbhome = os.path.join(dbhome, events_channel)
        if not os.path.exists(self._dbhome):
            os.mkdir(self._dbhome)

        self._readonly = readonly
        self._current_file = None
        self._current_day = None
        self._flash_memory = config.get(evtdao.CFGKEY_FLASH_MEM_SUPPORT, False)
        if self._flash_memory:
            self._logger.warning("flash memory support declared: systematic flush on write will be disabled")
        self._last_flush = 0

    def __enter__(self):
        return self

    def insert_event(self, msecs, var_type, var_name, data):
        """ See DAOObject class"""
        assert msecs
        assert var_type
        assert var_name
        assert data

        if self._readonly:
            msg = 'database opened in readonly'
            self._logger.error(msg)
            raise IOError(msg)

        # the time stamp is stored as a datetime type in the database, so that
        # we can benefit from available hi-level datetime manipulation
        # functions.
        # So we first create the UTC event timestamp from the passed
        # milliseconds count
        timestamp = datetime.utcfromtimestamp(msecs / 1000.0)

        if type(data) is dict:
            data_dict = data
        else:
            # convert the serialized data into a dictionary
            try:
                data_dict = json.loads(data)
            except ValueError as e:
                self._logger.error('malformed event data (%s): %s',
                                  data, e.message)
                return

        # move the value from the dictionary to a "first class" field
        try:
            value = data_dict[events.DataKeys.VALUE]
        except KeyError:
            self._logger.error('missing value field in data (%s)', data)
            return

        del data_dict[events.DataKeys.VALUE]
        json_data = json.dumps(data_dict)

        # check if we have to open a new storage file
        if timestamp.day != self._current_day:
            if self._current_file:
                self._current_file.close()
            self._current_file = open(self._get_path_for_day(timestamp.year,
                                                        timestamp.month,
                                                        timestamp.day), 'a')
            self._current_day = timestamp.day

        self._current_file.write(
            '\t'.join([timestamp.strftime(_TS_FMT),
                var_type,
                var_name,
                str(value),
                json_data]) + '\n')

        # Do not stress flash memories by too frequent physical writes, and let the
        # system driver do its job by optimizing this. There is a risk of loosing
        # some data in case of brutal stop (power loss f.i.) but it's better than
        # corrupting a whole SD card.
        now = time.time()
        if not self._flash_memory or (now - self._last_flush >= self.MAX_FLUSH_AGE):
            self._current_file.flush()
            self._last_flush = now

    def flush(self):
        """ Flushes the pending writes.
        """
        if self._current_file:
            self._current_file.flush()
            self._logger.info('on-demand data flush executed')
        else:
            self._logger.info('nothing to flush (no file currently in write mode)')

    def get_available_days(self, month=None):
        """ See DAOObject class"""
        if month and not isinstance(month, tuple):
            raise ValueError('month must be a tuple')

        for name in sorted([n for n in os.listdir(self._dbhome) if
                            n.endswith(_FILE_EXT)]):
            if month:
                yy, mm = month
                if yy > 2000:
                    yy -= 2000
                if not (int(name[0:2]) == yy and int(name[2:4]) == mm):
                    continue
            yield datetime.strptime(name[:6], _FNAME_DATE_FMT).date()

    def get_events_for_day(self, day, var_type=None, var_name=None):
        """ See DAOObject class"""
        self._logger.debug("get_events_for_day('%s','%s','%s') called" %
                           (day, var_type, var_name))

        if isinstance(day, date):
            yyyy, mm, dd = day.year, day.month, day.day
        else:
            yyyy, mm, dd = (int(x) for x in day[:10].replace('/', '-').split('-'))

        def ignore_corrupted_event(_rec_num, _record):
            self._logger.warning("ignoring corrupted event ([rec:%d] %s)" % (_rec_num, _record))

        fpath = self._get_path_for_day(yyyy, mm, dd)
        try:
            with open(fpath) as evtfile:
                rec_num = 0
                for record in evtfile:
                    rec_num += 1
                    try:
                        rec_ts, rec_var_type, rec_var_name, rec_value, rec_data = \
                            record.strip().split(_FLD_SEP)
                    except ValueError:
                        ignore_corrupted_event(rec_num, record)
                    else:
                        try:
                            rec_ts = datetime.strptime(rec_ts.ljust(20, '0'), _TS_FMT)
                        except (TypeError, ValueError):
                            ignore_corrupted_event(rec_num, record)
                        else:
                            if var_type and rec_var_type != var_type:
                                continue
                            if var_name and var_name != rec_var_name:
                                continue
                            try:
                                data = json.loads(rec_data)
                            except ValueError:
                                ignore_corrupted_event(rec_num, record)
                            else:
                                yield events.make_timed_event(
                                    rec_ts, rec_var_type, rec_var_name,
                                    value=rec_value,
                                    **data
                                )

        except IOError as e:
            self._logger.exception(e)

    def get_events(self, from_time=None, to_time=None, var_type=None, var_name=None):
        """ See DAOObject class"""
        self._logger.debug("get_events(%s,%s,%s,%s) called", from_time, to_time, var_type, var_name)

        from_day = from_time.date() if from_time else None
        to_day = to_time.date() if to_time else None

        if from_day or to_day:
            scanned_days = [
                day for day in self.get_available_days() \
                if (not from_day or day >= from_day) and \
                   (not to_day or day <= to_day)
            ]

        else:
            scanned_days = self.get_available_days()

        for day in scanned_days:
            sday = day.strftime(evtdao.TS_FMT_FULL)
            for event in self.get_events_for_day(sday):
                if from_time and \
                   day == scanned_days[0] and \
                   event.timestamp < from_time:
                    continue
                if to_time and day == scanned_days[-1] \
                   and event.timestamp > to_time:
                    continue
                if var_type and event.var_type != var_type:
                    continue
                if var_name and event.var_name != var_name:
                    continue

                yield event

    def _get_path_for_day(self, year, month, day):
        """ Returns the path of the storage file for a given date.

        A result is always returned, no matter if a file really exists for the
        given date.

        Parameters:
            year, month, day:
                the date

        Returns:
            the corresponding path
        """
        return os.path.join(self._dbhome,
                            "%02d%02d%02d%s" % (
                                year % 100, month, day, _FILE_EXT
                            )
                           )
