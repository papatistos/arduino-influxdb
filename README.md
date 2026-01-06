# Script to collect data from Arduino into InfluxDB

_Disclaimer: This is not an official Google product._

## Purpose

This script reads data from a serial device, typically Arduino, in InfluxDB's
[line protocol](https://docs.influxdata.com/influxdb/v1.2/write_protocols/line_protocol_tutorial/)
format and forwards it into an Influx database.

## Usage

Write an Arduino program that sends data in InfluxDB's format on the serial
line without timestamps (which are generally unavailable on Arduino). For
example:

    plant,pin=A15 moisture=140,temperature=27.4,humidity=67.3

Prepare an Influx database where you want to store the data. Then run:

    python3 collect.py -d /dev/ttyUSB0 -H localhost:8086 -D plants -T location=foo

This reads data from `/dev/ttyUSB0` and writes them to the database `plants`
running on `localhost:8086` (the default value for `-H`). It also adds tag
`location=foo` to each sample, in addition to the above `pin=A15` sent by
Arduino.

For detailed information about command line arguments run

    python3 collect.py --help

    python3 collect.py -q /tmp/queue.db ...

The queue ensures that no datapoints are ever lost, even if the database is
temporarily unreachable. It automatically performs garbage collection of 
acknowledged messages to prevent unbounded growth.

### Running with Telegraf

If the Influx database runs on a different machine, it might be helpful to run
[Telegraf](https://docs.influxdata.com/telegraf/v1.2/) locally. This has the
advantage that Telegraf can buffer messages in the case the connection to the
database fails, and also allows to collect monitoring data about the machine,
which is generally a good thing for long-running systems.

Keep in mind that Telegraph only allows posting data to a single database, the
one configured in Section `[[outputs.influxdb]]`. It ignores the database name
passed to it by `collect.py` (or any other script).

## Requirements

- **Python 3.6+**
- Python libraries:
  - [retrying](https://pypi.python.org/pypi/retrying)
  - [pyserial](https://pypi.python.org/pypi/pyserial)
  - [apsw](https://rogerbinns.github.io/apsw/) (for the persistent queue)
  - [paho-mqtt](https://pypi.org/project/paho-mqtt/) (optional, for MQTT support)

On Debian these can be installed using

    sudo apt-get install python3-retrying python3-serial python3-apsw python3-paho-mqtt

The persistent queue logic is included directly in this repository.

## MQTT Support

The script can optionally publish data to an MQTT broker. This is useful for integrating with
home automation systems like Home Assistant.

To enable MQTT, use the following arguments:
- `--mqtt-host`: Hostname or IP of the MQTT broker.
- `--mqtt-port`: Port (default 1883).
- `--mqtt-topic`: Base topic (default `arduino/data`).

Data is automatically grouped into sub-topics based on sensor suffixes (e.g., `.../outside`, `.../inside`).

## Contributions and future plans

Contributions welcome, please see [Code of Conduct](docs/code-of-conduct.md)
and [Contributing](docs/contributing.md). Currently I'd like to add:

- [Pytype](https://github.com/google/pytype) annotations.
- Thorough, proper testing.
- Packaging for Debian/Ubuntu.
