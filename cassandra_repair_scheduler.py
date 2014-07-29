#! /usr/bin/env python
"""
Run repairs on a regularly scheduled basis.  Drop this in cron, not more
often than hourly.

Workflow:
1) Insert a record (with TTL) into the queue table.
2) Select all records from the queue table.
   2a) If ours isn't the first one, delete ours and then exit.
3) Select all records from the status table.
4) If our status exists and is "running", delete our queue record and exit.
5) If our status exists and is "completed" and the completion time is too
   recent, delete our queue record and exit.
6) Create/replace our status with "running" (with TTL)
7) Delete our queue record
8) Run a repair
9) Replace our status with "completed" (with TTL)

"""

import sys
import logging
import argparse
import platform
import getpass
import time
import subprocess
import cql


class CqlWrapper(object):

    """Keep all of the CQL-specific stuff in here so we can have consistent
    retry handling, etc.

    Updates to SCHEMA may require updates to create_schema.
    """
    SCHEMA = [
        """CREATE KEYSPACE "{keyspace}"
             WITH replication = {{'class' : 'NetworkTopologyStrategy',
                                 {data_center_replication_map}}}
             AND durable_writes = false;
        """,
        """USE {keyspace};""",
        """CREATE TABLE "mutex" (
             nodename varchar,
             data_center varchar,
             PRIMARY KEY ((nodename), data_center))
           WITH comment='Poor MUTEX implementation'
        """,
        """CREATE TABLE "repair_status" (
             nodename varchar,
             data_center varchar,
             repair_status varchar,
             PRIMARY KEY ((nodename), data_center))
           WITH comment='Repair status of each node'
        """,
    ]
    GET_STATUS = """SELECT "repair_status" FROM "repair_status"
                    WHERE "nodename" = :nodename"""
    GET_LOCAL_STATUS = """SELECT "nodename", "repair_status" FROM "repair_status"
                          WHERE "data_center" = :data_center ALLOW FILTERING"""
    COMPLETED = "Completed"
    MUTEX_START = """INSERT INTO "mutex" ("nodename", "data_center")
                     VALUES (:nodename, :data_center) USING TTL :ttl"""
    MUTEX_CHECK = """SELECT "nodename", "data_center" FROM "mutex" """
    MUTEX_CLEANUP = """DELETE FROM "mutex" WHERE "nodename" = :nodename"""
    SELECT_ALL_DATACENTERS = """SELECT data_center FROM system.peers"""
    SELECT_MY_DATACENTER = """SELECT data_center FROM system.local"""
    REPAIR_START = """INSERT INTO "repair_status" ("nodename", "data_center", "repair_status")
                      VALUES (:nodename, :data_center, 'Started')"""
    REPAIR_UPDATE = """UPDATE "repair_status" SET "repair_status" = :newstatus
                       WHERE "nodename" = :nodename AND "data_center" = :data_center"""

    def __init__(self, option_group):
        """Set up and manage our connection.
        :param option_group: result of CLI parsing
        """
        self.option_group = option_group
        self.nodename = option_group.hostname
        self.conn = None
        try:
            self.standard_connection()
        except:
            self.create_schema()
        self.data_center = self.get_data_center()
        return

    def get_data_center(self):
        """Get our data center tag.
        :returns: data_center"""
        result = self.query_or_die(
            self.SELECT_MY_DATACENTER, "Looking for my datacenter")
        if not result:
            logging.fatal(
                "No data center in local data.  Still bootstrapping?")
            exit(1)
        return result[0][0]

    def standard_connection(self):
        """Set up a connection to Cassandra.
        """
        logging.debug('connecting to %s', self.option_group.keyspace)
        self.conn = cql.connect(self.option_group.hostname,
                                self.option_group.port,
                                self.option_group.keyspace,
                                user=self.option_group.username,
                                password=self.option_group.password,
                                cql_version=self.option_group.cqlversion)
        return

    def create_schema(self):
        """Creates the schema if it doesn't exist using the CQL in self.SCHEMA.
        Each query in there will be formatted with locals(), so if you
        update self.SCHEMA, be sure to update this function, too.
        """
        logging.info('creating schema')
        self.conn = cql.connect(self.option_group.hostname,
                                self.option_group.port,
                                "system",
                                user=self.option_group.username,
                                password=self.option_group.password,
                                cql_version=self.option_group.cqlversion)

        data_center = self.query_or_die(self.SELECT_ALL_DATACENTERS,
                                        "Unable to determine the local data center")
        if not data_center:
            logging.fatal(
                "No peers defined, repairs on a single-node cluster are silly")
            exit(0)

        # Cassandra doesn't support 'SELECT foo, 1 FROM ..." or DISTINCT,
        # so we have do something a little complicated to deduplicate the
        # results and then produce the desired string.
        data_center_replication_map = {}
        for row in data_center:
            data_center_replication_map[row[0]] = None
        data_center_replication_map = ", ".join(
            ["'%s':3" % x for x in data_center_replication_map])

        # This declaration is just so that "keyspace" will appear in locals.
        # pylint: disable=unused-variable
        keyspace = self.option_group.keyspace
        # pylint: enable=unused-variable
        for cql_query in self.SCHEMA:
            self.query(cql_query.format(**locals()))
        return

    def query_or_die(self, query_string, error_message, consistency_level="QUORUM", **kwargs):
        """Execute a query, on exception print an error message and exit.
        :param query_string: CQL to perform
        :param error_message: printed on error
        :param kwargs: dictionary to use for parameter substitution in the CQL
        """
        try:
            return self.query(query_string, consistency_level=consistency_level, **kwargs)
        except Exception as e:
            logging.fatal("%s: %s", error_message, e)
        exit(1)

    def query(self, query_string, consistency_level="QUORUM", **kwargs):
        """Execute a query.
        :param query_string: CQL to perform
        :param kwargs: dictionary to use for parameter substitution in the CQL
        :returns: query results
        """
        if not self.conn:
            self.standard_connection()
        cursor = self.conn.cursor()
        logging.debug("Query: %s, arguments: %s", query_string, str(kwargs))
        cursor.execute(
            query_string.encode('ascii'), kwargs, consistency_level=consistency_level)
        data = cursor.fetchall()
        cursor.close()
        logging.debug(str(data))
        return data

    def close(self):
        """Shut down the connection gracefully."""
        self.conn.close()
        self.conn = None
        return

    def check_should_run(self):
        """Check to see if it is appropriate to start up.
        :returns: boolean
        """
        logging.debug("Check to see if we're already running a repair")
        result = self.query_or_die(
            self.GET_STATUS, "Checking status", nodename=self.nodename)
        # If there's any result at all, either a run is in progress, or the
        # last completed run hasn't expired yet.  Either way, bail.
        if result:
            logging.info("Repair in progress: %s", result[0][0])
            return False

        logging.debug("Check to see if anyone else in the local ring is running a repair")
        result = self.query_or_die(self.GET_LOCAL_STATUS,
                                   "Checking local ring status",
                                   nodename=self.nodename,
                                   data_center=self.data_center)

        if result:
            already_running = [x[0] for x in result if x[1] != self.COMPLETED]
            if already_running:
                logging.info("Another node is repairing.: %s", already_running[0])
                return False
        self.query_or_die(self.MUTEX_START, "Starting MUTEX",
                          nodename=self.nodename,
                          data_center=self.data_center,
                          ttl=self.option_group.ttl)
        # Totally arbitrary delay here, because I don't trust C*.
        logging.debug('Five second pause here')
        time.sleep(5)
        result = self.query_or_die(self.MUTEX_CHECK, "Checking MUTEX",
                                   consistency_level="ONE",
                                   data_center=self.data_center)
        if not result or not [x[0] for x in result if x[1] == self.data_center][0] == self.nodename:
            self.query(self.MUTEX_CLEANUP, nodename=self.nodename)
            return False
        return True

    def claim_repair(self):
        """Insert a row claiming that we're starting the repair,
        then remove the MUTEX."""
        self.query_or_die(self.REPAIR_START,
                          "Starting Repair", nodename=self.nodename,
                          data_center=self.data_center)
        self.query_or_die(self.MUTEX_CLEANUP,
                          "Dropping MUTEX record",
                          nodename=self.nodename)
        self.close()
        return

    def run_repair(self):
        """Run the entire repair"""
        cmd = [self.option_group.range_repair_tool,
               "-D", self.data_center,
               "-H", self.nodename,
               "--dry-run"]     # So we get a list of commands to run.

        logging.debug("geting repair steps, this may take a while")
        repair_steps = subprocess.check_output(cmd).split('\n')
        for line in repair_steps:
            if not line:
                continue
            step, repair_command = line.split(" ", 1)
            self.query(self.REPAIR_UPDATE, nodename=self.nodename,
                       newstatus=step, data_center=self.data_center)
            self.close()        # Individual repairs may be slow
            logging.debug(repair_command)
            subprocess.call(repair_command, shell=True)
        self.query(
            self.REPAIR_UPDATE, nodename=self.nodename, newstatus=self.COMPLETED)
        return


def setup_logging(option_group):
    """Sets up logging in a syslog format by log level
    :param option_group: options as returned by the OptionParser
    """
    stderr_log_format = "%(levelname) -8s %(asctime)s %(funcName)s line:%(lineno)d: %(message)s"
    file_log_format = "%(asctime)s - %(levelname)s - %(message)s"
    logger = logging.getLogger()
    if option_group.debug:
        logger.setLevel(level=logging.DEBUG)
    elif option_group.verbose:
        logger.setLevel(level=logging.INFO)
    else:
        logger.setLevel(level=logging.WARNING)

    # First, clear out any default handlers
    for handler in logger.handlers:
        logger.removeHandler(handler)

    if option_group.syslog:
        # Use standard format here because timestamp and level will be added by
        # syslogd.
        logger.addHandler(logging.SyslogHandler(facility=option_group.syslog))
    if option_group.logfile:
        logger.addHandler(logging.FileHandler(option_group.logfile))
        logger.handlers[-1].setFormatter(logging.Formatter(file_log_format))
    if not logger.handlers:
        logger.addHandler(logging.StreamHandler())
        logger.handlers[-1].setFormatter(logging.Formatter(stderr_log_format))
    return


def cli_parsing():
    """Parse the command line.
    :returns: option set
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action='store_true',
                        default=False, help="Verbose output")
    parser.add_argument("-d", "--debug", action='store_true',
                        default=False, help="Debugging output")
    parser.add_argument("--syslog", metavar="FACILITY",
                        help="Send log messages to the syslog")
    parser.add_argument("--logfile", metavar="FILENAME",
                        help="Send log messages to a file")
    parser.add_argument("-H", "--hostname", default=platform.node(),
                        help="Hostname (default: %(default)s)")
    parser.add_argument("-p", "--port", default=9160, type=int,
                        help="Port (default: %(default)d)")
    parser.add_argument("-U", "--username",
                        help="Username (if necessary)")
    parser.add_argument("-P", "--password",
                        help="Password. (prompt if user provided but not password)")
    parser.add_argument("-t", "--ttl", default=3600 * 24 * 20, type=int,
                        help="TTL (default: %(default)d)")
    parser.add_argument("-k", "--keyspace", default="operations",
                        help="Keyspace to use (default: %(default)s)")
    parser.add_argument("--cqlversion", default="3.0.5",
                        help="CQL version (default: %(default)s)")
    parser.add_argument("-r", "--range_repair_tool",
                        default="/usr/local/bin/range_repair.py",
                        help="Range repair tool path (default: %(default)s)")
    options = parser.parse_args()
    setup_logging(options)
    if options.username and not options.password:
        options.password = getpass.getpass(
            'Password for %s: ' % options.username)
    return options


def main():
    """Main entry point.  Runs the actual program here."""
    logging.debug('main')
    options = cli_parsing()
    connection = CqlWrapper(options)
    if connection.check_should_run():
        connection.claim_repair()
        # Arguably, this should not be done in the connection.
        connection.run_repair()
    connection.close()

    return


if __name__ == '__main__':
    main()
