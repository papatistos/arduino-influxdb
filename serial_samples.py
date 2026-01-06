# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Reads and parses lines from a serial device.

Typically from an Arduino. Lines are expected to follow the InfluxDB's line
protocol format (with the difference that the timestamp is allowed to be
missing).
"""

import logging
import time
from typing import BinaryIO, Generator


def SkipUntilNewLine(handle: BinaryIO):
    """Skips data until a new-line character is received.

    This is needed so that the first sample is read from a complete line.
    """
    logging.debug("Skipping until the end of a new line.")
    while not handle.readline(4096).endswith(b"\n"):
        pass


class LineOverflowError(IOError):
    """Thrown when a line longer than a given limit is received."""

    def __init__(self, line: bytes, max_line_length: int):
        if not line:
            message = "Timeout on device inactivity, no data received"
        elif len(line) >= max_line_length:
            message = "Read line overflow: {0!r}".format(line)
        else:
            message = "Read timeout; received incomplete line: {0!r}".format(
                line)
        super().__init__(message)


def SerialLines(handle: BinaryIO,
                max_line_length: int) -> Generator[bytes, None, None]:
    """A generator that yields lines from a configured serial line.

    Will never exit normally, only with an exception when there is an error
    in the serial communication.
    """
    SkipUntilNewLine(handle)
    while True:
        line: bytes = handle.readline(max_line_length)
        logging.debug("Received line %r", line)
        if not line.endswith(b"\n"):
            raise LineOverflowError(line, max_line_length)
        yield line.rstrip()

def check_incomplete_line(handle: BinaryIO, timeout: float, max_line_length: int):
    '''Attempt to read a line within the given timeout starting from the first received byte.
       Returns an incomplete line if timeout expires.'''
    line = b""
    end_time = None
    while len(line) < max_line_length:
        if handle.readable():
            char = handle.read(1)  # read one byte
            if end_time is None:  # if this is the first received byte
                end_time = time.time() + timeout  # set the end time
            line += char
            if char == b'\n':
                return line
            elif  (time.time() >= end_time):
                logging.warning("Dismissing incomplete line: %r", line)
                return None # dont return the incomplete line

def SerialLines(handle: BinaryIO, max_line_length: int, timeout: float) -> Generator[bytes, None, None]:
    """A generator that yields lines from a configured serial line.
       Will never exit normally, only with an exception when there is an error
       in the serial communication.
    """
    SkipUntilNewLine(handle)
    while True:
        # replaced the handle.readline() call with our custom function
        line: bytes = check_incomplete_line(handle, timeout, max_line_length)
        logging.debug("Received line %r", line)
        if line is None:
            logging.warning('Received a None line')
            continue  # Skip this iteration and move to the next one
        if not line.endswith(b"\n"):
            raise LineOverflowError(line, max_line_length)
        yield line.rstrip()
