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
