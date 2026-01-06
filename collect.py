import argparse
import importlib
import logging
from logging.handlers import RotatingFileHandler
import sys
import threading
import time
import datetime
import json
import paho.mqtt.client as mqtt
from typing import BinaryIO, Callable, FrozenSet, Generator, Optional, Dict, Any

from retrying import retry
import serial

import influxdb
import persistent_queue
import serial_samples

import sched
import subprocess
import time
import os

def restart_script():
    logging.info("Restarting script...")
    os._exit(1)  # Force immediate exit

def RetryOnIOError(exception):
    """Returns True if 'exception' is an IOError."""
    return isinstance(exception, IOError)

def parse_influx_line_to_dict(line_str: str) -> Dict[str, Any]:
    """Parses an InfluxDB line protocol string into a flat dictionary."""
    # Format: measurement,tag1=val1 field1=val1,field2=val2 timestamp
    try:
        parts = line_str.split(" ")
        if len(parts) < 2:
            return {}
        
        # Part 0: measurement + tags
        meas_parts = parts[0].split(",")
        measurement = meas_parts[0]
        tags = {}
        for t in meas_parts[1:]:
            if "=" in t:
                k, v = t.split("=", 1)
                tags[k] = v

        # Part 1: fields
        fields_str = parts[1]
        fields = {}
        for f in fields_str.split(","):
            if "=" in f:
                k, v = f.split("=", 1)
                try:
                    if v.endswith("i"):
                        fields[k] = int(v[:-1])
                    elif v.lower() in ["true", "false", "t", "f"]:
                         fields[k] = (v.lower() in ["true", "t"])
                    elif '"' in v:
                        fields[k] = v.strip('"')
                    else:
                        fields[k] = float(v)
                except ValueError:
                    fields[k] = v

        # Part 2: timestamp (optional)
        timestamp = None
        if len(parts) > 2:
            try:
                timestamp = int(parts[2])
            except ValueError:
                pass

        data = {
            "measurement": measurement,
            **tags,
            **fields
        }
        if timestamp:
            data["timestamp"] = timestamp
            
        return data
    except Exception as e:
        logging.warning(f"Failed to parse line for JSON conversion: {line_str}. Error: {e}")
        return {}

def group_mqtt_data(flat_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Groups flat data into sub-topics with renamed fields."""
    groups = {
        "outside": {},
        "incoming": {},
        "inside": {},
        "outgoing": {},
        "stats": {},
        "system": {}
    }
    
    # Use timezone-aware timestamp for Home Assistant compatibility
    timestamp_iso = datetime.datetime.now().astimezone().isoformat()
    
    # Mapping definitions
    sensor_groups = [
        ("0", "outside"),
        ("1", "incoming"),
        ("2", "inside"),
        ("3", "outgoing")
    ]
    
    prefix_map = {
        "t": "temp",
        "rh": "hum",
        "ah": "ah",
        "dew": "dew"
    }

    stats_keys = ["h-eff", "t-eff", "hum-gain"]

    for k, v in flat_data.items():
        if k in ["measurement", "timestamp"]:
            # Add measurement to system
            if k == "measurement":
                groups["system"]["measurement"] = v
            continue

        matched = False
        
        # Check sensor groups
        for suffix, group in sensor_groups:
            if k.endswith(suffix):
                prefix = k[:-1]
                if prefix in prefix_map:
                    new_key = f"{group}_{prefix_map[prefix]}"
                    groups[group][new_key] = v
                    matched = True
                    break
        if matched:
            continue

        # Check stats
        if k in stats_keys or k.startswith("CR"):
            groups["stats"][k] = v
            continue
            
        # Default to system
        groups["system"][k] = v

    # Add timestamp to all groups that have data
    final_groups = {}
    for g_name, g_data in groups.items():
        if g_data:
            g_data["last_updated"] = timestamp_iso
            final_groups[g_name] = g_data
            
    return final_groups


@retry(wait_exponential_multiplier=1000,
       wait_exponential_max=60000,
       retry_on_exception=RetryOnIOError)
def ReadLoop(args, queue: persistent_queue.Queue):
    """Reads samples and stores them in a queue. Retries on IO errors."""
    serial_fn: Optional[Callable[[BinaryIO], Generator[bytes, None,
                                                       None]]] = None
    if args.serial_function:
        module, fn_name = args.serial_function.rsplit(".", 1)
        serial_fn = getattr(importlib.import_module(module), fn_name)

    try:
        logging.debug("Read loop started")
        with serial.serial_for_url(args.device,
                                   baudrate=args.baud_rate,
                                   timeout=args.read_timeout) as handle:
            if serial_fn:
                lines = serial_fn(handle)
            else:
                lines = serial_samples.SerialLines(handle, args.max_line_length, 10.0)
            start_time = time.time()

            for line in lines:
                # If line does not start with start string
                if not line.startswith(b'FTX'):
                   continue

                try:
                    line = str(line, encoding="UTF-8")
                except UnicodeDecodeError:
                    logging.warning("Skipping line with invalid UTF-8: %r", line)
                    continue
                except TypeError:
                    pass
                
                # Parse 'line', either with or without timestamp.
                try:
                    words = line.strip().split(" ")
                    if len(words) == 2:
                        (tags, values) = words
                        timestamp = int(time.time() * 1000000000)
                    elif len(words) == 3:
                        (tags, values, timestamp) = words
                        float(timestamp)
                    else:
                        raise ValueError("Unable to parse line {0!r}".format(line))
                except Exception as e:
                     logging.warning(f"Error parsing line structure: {e}")
                     continue

                # Filter out values that are 'nan' as InfluxDB does not support them.
                value_parts = values.split(',')
                valid_values = []
                removed_fields = []
                for part in value_parts:
                    if '=' in part:
                        key, val = part.split('=', 1)
                        # Remove nan values
                        if val.lower() == 'nan':
                            removed_fields.append(key)
                            continue
                        
                        # Remove zero humidity values (sensor error)
                        if key.startswith('rh'):
                            try:
                                if float(val) == 0:
                                    removed_fields.append(key)
                                    continue
                            except ValueError:
                                pass

                    valid_values.append(part)

                if removed_fields:
                    try:
                        line_str = line.strip()
                    except:
                        line_str = str(line)
                    logging.getLogger('data_errors').warning(f"Removed nan values for fields {removed_fields} from line: {line_str}")

                if not valid_values:
                    logging.warning("Skipping line because all values are nan: %r", line)
                    continue
                values = ",".join(valid_values)

                tags: str = ",".join(t for t in (tags, args.tags) if t)
                queue.put("{0} {1} {2:d}".format(tags, values, timestamp))

    except:
        logging.exception("Error, retrying with backoff")
        raise
        

@retry(wait_exponential_multiplier=1000,
       wait_exponential_max=60000,
       retry_on_exception=RetryOnIOError)
def WriteLoop(args, queue: persistent_queue.Queue):
    """Reads samples and stores them in a queue. Retries on IO errors."""
    logging.debug("Write loop started")
    warn_on_status: FrozenSet[int] = frozenset(
        int(status) for status in args.warn_on_status)
    
    mqtt_client = None
    if args.mqtt_host:
        try:
            mqtt_client = mqtt.Client()
            mqtt_client.connect(args.mqtt_host, args.mqtt_port, 60)
            mqtt_client.loop_start()
            logging.info(f"Connected to MQTT broker at {args.mqtt_host}:{args.mqtt_port}")
        except Exception as e:
            logging.error(f"Failed to connect to MQTT broker: {e}")
            mqtt_client = None

    try:
        with influxdb.InfluxdbClient(args.host, args.database) as influx_client:
            influxdb_line: str
            count = 0
            for influxdb_line in queue.get_blocking(tick=60):
                # Send to InfluxDB
                influx_client.post_lines([influxdb_line.encode(encoding='UTF-8')],
                                         warn_on_status=warn_on_status)
                
                # Send to MQTT (best effort)
                if mqtt_client:
                    try:
                        flat_data = parse_influx_line_to_dict(influxdb_line)
                        if flat_data:
                            grouped_data = group_mqtt_data(flat_data)
                            for subtopic, payload in grouped_data.items():
                                full_topic = f"{args.mqtt_topic}/{subtopic}"
                                # Trim trailing slash if base topic is empty or ends with slash? 
                                # Usually args.mqtt_topic is 'arduino/data', so 'arduino/data/outside'
                                full_topic = full_topic.replace("//", "/")
                                mqtt_client.publish(full_topic, json.dumps(payload))
                    except Exception as e:
                        logging.warning(f"Failed to publish to MQTT: {e}")

                count += 1
                if count % 1000 == 0:
                    try:
                        queue.garbage_collect()
                    except Exception as e:
                        logging.warning(f"Garbage collection failed: {e}")

    except:
        logging.exception("Error, retrying with backoff")
        if mqtt_client:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        raise


def RunAndDie(fun, *args):
    """Runs 'fn' on 'args'. If 'fn' exists, exit the whole program."""
    try:
        fun(*args)
    finally:
        sys.exit(1)

def main():
    """Parses the command line arguments and invokes the main loop."""
    
    # Load configuration from config.json if it exists
    config = {}
    config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.json')
    print(f"Attempting to load config from: {config_path}")
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                print(f"Loaded configuration from {config_path}")
        except Exception as e:
            print(f"Failed to load config.json: {e}")

    mqtt_config = config.get('mqtt', {})

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="""
          Collects values from a serial port and sends them to InfluxDB.
          ...
        """,
        # ... existing epilog ...
        epilog="""See
          https://pyserial.readthedocs.io/en/latest/url_handlers.html#urls
          for URL types accepted by -d/--device.
          Run `python -m serial.tools.list_ports` to list of all available
          COM ports.
        """)
    # ... existing arguments ...
    parser.add_argument('-d',
                        '--device',
                        required=True,
                        help='serial device to read from, or a URL accepted '
                        'by serial_for_url()')
    parser.add_argument('-r',
                         '--baud-rate',
                        type=int,
                        default=9600,
                        help='baud rate of the serial device')
    parser.add_argument('-t',
                        '--read-timeout',
                        type=int,
                        required=True,
                        help='read timeout on the serial device; this should '
                        'be longer that the longest expected period of '
                        'inactivity of the serial device')
    parser.add_argument('--max-line-length',
                        type=int,
                        default=1024,
                        help='maximum line length')
    parser.add_argument('--serial-function',
                        help='custom function that reads from a serial device '
                        'passed as its argument and yields InfluxDB '
                        'lines; specified as module.functionname')

    parser.add_argument('-H',
                        '--host',
                        default='localhost:8086',
                        help='host and port with InfluxDB to send data to')
    parser.add_argument('-D',
                        '--database',
                        required=True,
                        help='database to save data to')
    parser.add_argument('-T',
                        '--tags',
                        default='',
                        help='additional static tags for measurements'
                        ' separated by comma, for example foo=x,bar=y')
    parser.add_argument('--warn_on_status',
                        nargs='*',
                        default=[400],
                        help='when one of these HTTP statuses is received from'
                        ' InfluxDB, a warning is printed and the'
                        ' datapoint is skipped; allows to continue on'
                        ' invalid datapoints')

    parser.add_argument('-q',
                        '--queue',
                        default=':memory:',
                        help='path for a persistent queue database file; this '
                        'file will be automatically created and managed '
                        'by the program; it ensures that no datapoints '
                        'are ever lost, even if the database is '
                        'temporarily unreachable; acknowledged messages '
                        'are automatically garbage collected.')
    parser.add_argument('-w',
                        '--wal_autocheckpoint',
                        type=int,
                        default=10,
                        help='switches the queue SQLite database to use the '
                        'WAL mode and sets this parameter in the database')

    parser.add_argument('--debug',
                        action='store_true',
                        help='enable debug level')
    
    # MQTT Arguments (defaults from config if available)
    parser.add_argument('--mqtt-host',
                        default=mqtt_config.get('host'),
                        help='MQTT broker hostname or IP address')
    parser.add_argument('--mqtt-port',
                        type=int,
                        default=mqtt_config.get('port', 1883),
                        help='MQTT broker port (default 1883)')
    parser.add_argument('--mqtt-topic',
                        default=mqtt_config.get('topic', 'arduino/data'),
                        help='MQTT topic to publish JSON data to')

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    # Setup dedicated logger for data errors (nan values) to keep them separate but safe
    data_error_logger = logging.getLogger('data_errors')
    data_error_logger.setLevel(logging.WARNING)
    # Rotate log after 5MB, keep 1 backup file
    handler = RotatingFileHandler('error.log', maxBytes=5*1024*1024, backupCount=1)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    data_error_logger.addHandler(handler)
    # Prevent these from propagating to the root logger (and console/main log) if desired
    data_error_logger.propagate = False

    # Create a scheduler and schedule the script to restart in 24 hours
    scheduler = sched.scheduler(time.time, time.sleep)
    restart_delay = 24 * 60 * 60  # 24 hours in seconds
    scheduler.enter(restart_delay, 1, restart_script)

    # Start the scheduler in a separate thread
    scheduler_thread = threading.Thread(target=scheduler.run)
    scheduler_thread.start()

    while True:
        try:
            with persistent_queue.Queue(
                    args.queue, wal_autocheckpoint=args.wal_autocheckpoint) as queue:
                reader = threading.Thread(name="read",
                                          target=ReadLoop,
                                          args=(args, queue))
                writer = threading.Thread(name="write",
                                          target=WriteLoop,
                                          args=(args, queue))
                reader.start()
                writer.start()
                reader.join()
                writer.join()
        except Exception:
            logging.exception("Unhandled exception occurred, restarting script...")
            time.sleep(5)  # Wait a bit before restarting
            continue
        break  # Exit the loop if no exceptions occurred

if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception:
            logging.exception("Unhandled exception occurred in main(), restarting script...")
            time.sleep(5)  # Wait a bit