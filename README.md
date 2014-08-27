cassandra_tools
=======

# casstop

"top"-like tool for Cassandra.  It attempts to enable a real-time view into the state of your Cassandra cluster.  Requires http://mx4j.sourceforge.net/.

## Usage
```
casstop $NODENAME [$NODENAME ...]
```

# stop_cassandra_repairs

Cassandra repairs have an unfortunate tendency to hang, but there are no tools to kill off such a hung repair, thus tying up resources on the problem nodes until such time as they are restarted.  stop_cassandra_repairs will use MX4J to stop any outstanding repairs on the nodes you give it.  Requires http://mx4j.sourceforge.net/.

##Usage
```
stop_cassandra_repairs $HUNG_NODE [$HUNG_NODE ...]
```

# cassandra_repair_scheduler.py

Script for scheduling repairs on your cluster.  Requires the use of
https://github.com/BrianGallew/cassandra_range_repair to work.

##Basic Usage
```
echo 0 */4 * * * /usr/local/bin/cassandra_repair_scheduler.py >> /etc/crontab
```

##Help
```
usage: cassandra_repair_scheduler.py [-h] [-v] [-d] [--syslog FACILITY]
                                     [--logfile FILENAME] [-H HOSTNAME]
                                     [-p PORT] [-U USERNAME] [-P PASSWORD]
                                     [-t TTL] [-k KEYSPACE]
                                     [--cqlversion CQLVERSION]
                                     [-r RANGE_REPAIR_TOOL]

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         Verbose output
  -d, --debug           Debugging output
  --syslog FACILITY     Send log messages to the syslog
  --logfile FILENAME    Send log messages to a file
  -H HOSTNAME, --hostname HOSTNAME
                        Hostname (default: mactheknife.local)
  -p PORT, --port PORT  Port (default: 9160)
  -U USERNAME, --username USERNAME
                        Username (if necessary)
  -P PASSWORD, --password PASSWORD
                        Password. (prompt if user provided but not password)
  -t TTL, --ttl TTL     TTL (default: 1728000)
  -k KEYSPACE, --keyspace KEYSPACE
                        Keyspace to use (default: operations)
  --cqlversion CQLVERSION
                        CQL version (default: 3.0.5)
  -r RANGE_REPAIR_TOOL, --range_repair_tool RANGE_REPAIR_TOOL
                        Range repair tool path (default:
                        /usr/local/bin/range_repair.py)
  --watch               See the live repair status.
```

# poison_pill_tester

Script for discovering wide rows.  You'll need to convert your data to JSON, first.

##Usage
```
 nodetool snapshot keyspace suspect_column_family
 #(cd into the relevant snapshot directory)
 for d in *-Data.db
 do
   e=$(echo $d | sed s,-Data.db,.json,)
   sstable2json $d > $e
 done
 poison_pill_tester *.json
```

# MX4J

http://mx4j.sourceforge.net/ is, among other things, a JMX<->HTML bridge.

## Installation

MX4J is available via "apt-get install libmx4j-java" on Ubuntu (that's
where it's current used).

Once that library is installed, you need to ensure that it's in Cassandra's
load path, most easily done by symlinking it like this:

```
ln -s /usr/share/java/mx4j-tools.jar /usr/share/cassandra/lib/mx4j-tools.jar
```

Finally, you need to load it into cassandra.  Edit cassandra-env.sh and add
the following line:

```
JVM_OPTS="${JVM_OPTS} -Dmx4jaddress=$(ip addr show dev eth0 | grep 'inet ' | sed -e s,inet,, -e 's,/.*,,')"
```

NB: that assumes you have only one address on eth0, and that eth0 is what
you want MX4J listening on.
