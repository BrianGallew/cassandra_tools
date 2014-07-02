cassandra_tools
=======

# casstop:

"top"-like tool for Cassandra.  It attempts to enable a real-time view into the state of your Cassandra cluster.  Requires http://mx4j.sourceforge.net/.

Usage: casstop $NODENAME [$NODENAME ...]

# stop_cassandra_repairs

Cassandra repairs have an unfortunate tendency to hang, but there are no tools to kill off such a hung repair, thus tying up resources on the problem nodes until such time as they are restarted.  stop_cassandra_repairs will use MX4J to stop any outstanding repairs on the nodes you give it.  Requires http://mx4j.sourceforge.net/.

Usage: stop_cassandra_repairs $HUNG_NODE [$HUNG_NODE ...]

