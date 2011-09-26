"""
Test txMySQL against a local MySQL server

This test requires 'sudo' without a password, and expects a stock Ubuntu 10.04
MySQL setup. It will start and stop MySQL and occasionally replace it with an
evil daemon which absorbs packets.

TODO: Check code coverage for every line, then manually any compound expression
in a conditional to check that there is test case coverage for each case.

Please CREATE DATABASE foo and grant the appropriate credentials before running
this test suite.
"""
import os, pwd, sys
from errno import ENOENT

from twisted.python.filepath import FilePath
from twisted.trial import unittest
from twisted.internet import defer, reactor
from twisted.internet.base import DelayedCall
from twisted.internet.error import ConnectionDone

DelayedCall.debug = False
from txmysql import client
from HybridUtils import AsyncExecCmds, sleep
import secrets

if 'freebsd' in sys.platform:
    onFreeBSD = True
    skipReason = "Test only works on Ubuntu"
else:
    onFreeBSD = False
    skipReason = "Test only works on FreeBSD"

FREEBSD_TESTS = []


class MySQLClientTest(unittest.TestCase):

    @defer.inlineCallbacks
    def test_0004_cleanup_prepared_statements(self):
        """
        Checks that when there are no pending or current operations that we
        disconnect and stay disconnected.
        You must set max_prepared_stmt_count = 100 in /etc/mysql/my.cnf for
        this to actually get tested.
        """
        yield self._start_mysql()
        conn = self._connect_mysql()
        for i in range(200):
            if i % 100 == 0:
                print 'Done %i queries' % i
            res = yield conn.runQuery("select 1")
            self.assertEquals(res, [[1]])
        conn.disconnect()

    @defer.inlineCallbacks
    def test_0005_query_timeout_stay_disconnected(self):
        """
        Checks that when there are no pending or current operations that we
        disconnect and stay disconnected
        """
        yield self._start_mysql()
        conn = self._connect_mysql(retry_on_error=True, idle_timeout=2)
        res = yield conn.runQuery("select 1")
        yield sleep(6)
        self.assertIdentical(conn.client, None)
        conn.disconnect()
    
    @defer.inlineCallbacks
    def test_0010_two_queries_disconnected(self):
        yield self._start_mysql()
        conn = self._connect_mysql(retry_on_error=True, idle_timeout=1)
        yield conn.runQuery("select 1")
        yield sleep(2)
        a = conn.runQuery("select 2")
        b = conn.runQuery("select 3")
        a, b = yield defer.gatherResults([a, b])
        self.assertEquals(a, [[2]])
        self.assertEquals(b, [[3]])
        self.assertEquals((yield conn.runQuery("select 4")), [[4]])
        conn.disconnect()

    @defer.inlineCallbacks
    def test_0020_start_query_restart(self):
        yield self._start_mysql()
        conn = self._connect_mysql(retry_on_error=True, idle_timeout=2)
        result = yield conn.runQuery("select 2")
        #yield self._stop_mysql()
        #yield self._start_mysql()
        yield sleep(10)
        conn.disconnect()
        self.assertEquals(result, [[2]])

    def test_0030_escaping(self):
        try:
            client._escape("%s", ())
            self.fail("that should have raised an exception")
        except TypeError:
            pass

        try:
            client._escape("select * from baz baz baz", (1, 2, 3))
            self.fail("that should have raised an exception")
        except TypeError:
            pass

        result = client._escape("update foo set bar=%s where baz=%s or bash=%s", ("%s", "%%s", 123))
        self.assertEquals(result, "update foo set bar='%s' where baz='%%s' or bash='123'")

    @defer.inlineCallbacks
    def test_0040_thrash(self):
        yield self._start_mysql()
        conn = self._connect_mysql(retry_on_error=True)
        yield conn.runOperation("drop table if exists thrashtest")
        yield conn.runOperation("create table thrashtest (id int)")

        dlist = []
        for i in range(100):
            dlist.append(conn.runOperation("insert into thrashtest values (%s)", [i]))
        yield defer.DeferredList(dlist)

        dlist = []
        for i in range(50):
            print "Appending %i" % i
            dlist.append(conn.runQuery("select sleep(0.1)"))
            dlist.append(conn.runQuery("select * from thrashtest where id=%s", [i]))

        yield sleep(3)

        print "About to stop MySQL"
        dstop = self._stop_mysql()
        def and_start(data):
            print "About to start MySQL"
            return self._start_mysql()
        dstop.addCallback(and_start)
        
        for i in range(50,100):
            print "Appending %i" % i
            dlist.append(conn.runQuery("select sleep(0.1)"))
            dlist.append(conn.runQuery("select * from thrashtest where id=%s", [i]))

        results = yield defer.DeferredList(dlist)
        print results

        conn.disconnect()
        #self.assertEquals(result, [[1]])


    @defer.inlineCallbacks
    def test_0050_test_initial_database_selection(self):
        """
        Check that when we connect to a database in the initial handshake, we
        end up in the 'foo' database. TOOD: Check that we're actually in the
        'foo' database somehow.
        """
        yield self._start_mysql()
        conn = self._connect_mysql(database='foo')
        result = yield conn.runOperation("create table if not exists foo (id int primary key)")
        result = yield conn.runOperation("delete from foo.foo")
        result = yield conn.runOperation("insert into foo.foo set id=191919")
        result = yield conn.runQuery("select * from foo order by id desc limit 1")
        conn.disconnect()
        self.assertEquals(result, [[191919]])

    @defer.inlineCallbacks
    def test_0100_start_connect_query(self):
        """
        1. Start MySQL
        2. Connect
        3. Query - check result
        """
        yield self._start_mysql()
        conn = self._connect_mysql()
        result = yield conn.runQuery("select 2")
        conn.disconnect()
        self.assertEquals(result, [[2]])

    @defer.inlineCallbacks
    def test_0200_stop_connect_query_start(self):
        """
        1. Connect, before MySQL is started
        2. Start MySQL
        3. Query - check result
        XXX The comment is correct but the code is wrong!
        """
        conn = self._connect_mysql()
        d = conn.runQuery("select 2") # Should get connection refused, because we're not connected right now
        yield self._start_mysql()
        result = yield d
        conn.disconnect()
        self.assertEquals(result, [[2]])

    @defer.inlineCallbacks
    def test_0210_stop_connect_query_start_retry_on_error(self):
        """
        1. Connect, before MySQL is started
        2. Start MySQL
        3. Query - check result
        """
        conn = self._connect_mysql(retry_on_error=True)
        d = conn.runQuery("select 2")
        yield self._start_mysql()
        result = yield d
        conn.disconnect()
        self.assertEquals(result, [[2]])

    @defer.inlineCallbacks
    def test_0211_stop_connect_query_start_retry_on_error_two_queries(self):
        """
        1. Connect, before MySQL is started
        2. Start MySQL
        3. Query - check result
        """
        conn = self._connect_mysql(retry_on_error=True)
        d = conn.runQuery("select 2")
        d2 = conn.runQuery("select 3")
        yield self._start_mysql()
        result = yield d
        result2 = yield d2
        conn.disconnect()
        self.assertEquals(result, [[2]])
        self.assertEquals(result2, [[3]])

    @defer.inlineCallbacks
    def test_0300_start_idle_timeout(self):
        """
        Connect, with evildaemon in place of MySQL
        Evildaemon stops in 5 seconds, which is longer than our idle timeout
        so the idle timeout should fire, disconnecting us.
        But because we have a query due, we reconnect and get the result.
        """
        daemon_dfr = self._start_evildaemon(secs=10)
        conn = self._connect_mysql(idle_timeout=3, retry_on_error=True)
        d = conn.runQuery("select 2")
        yield daemon_dfr
        yield self._start_mysql()
        result = yield d
        conn.disconnect()
        self.assertEquals(result, [[2]])

    @defer.inlineCallbacks
    def test_0400_start_connect_long_query_timeout(self):
        """
        Connect to the real MySQL, run a long-running query which exceeds the
        idle timeout, check that it times out and returns the appropriate
        Failure object (because we haven't set retry_on_error)
        """
        yield self._start_mysql()
        conn = self._connect_mysql(idle_timeout=3)
        try:
            result = yield conn.runQuery("select sleep(5)")
        except Exception, e:
            print "Caught exception %s" % e
            self.assertTrue(isinstance(e, ConnectionDone))
        finally:
            conn.disconnect()
        
    @defer.inlineCallbacks
    def test_0500_retry_on_error(self):
        """
        Start a couple of queries in parallel.
        Both of them should take 10 seconds, but restart the MySQL
        server after 5 seconds.
        Setting the connection and idle timeouts allows bad connections
        to fail.
        """
        yield self._start_mysql()
        conn = self._connect_mysql(retry_on_error=True)
        d1 = conn.runQuery("select sleep(7)")
        d2 = conn.runQuery("select sleep(7)")
        yield sleep(2)
        yield self._stop_mysql()
        yield self._start_mysql()
        result = yield defer.DeferredList([d1, d2])
        conn.disconnect()
        self.assertEquals(result, [(True, [[0]]), (True, [[0]])])

    @defer.inlineCallbacks
    def test_0550_its_just_one_thing_after_another_with_you(self):
        """
        Sanity check that you can do one thing and then another thing.
        """
        yield self._start_mysql()
        conn = self._connect_mysql(retry_on_error=True)
        yield conn.runQuery("select 2")
        yield conn.runQuery("select 2")
        conn.disconnect()

    @defer.inlineCallbacks
    def test_0600_error_strings_test(self):
        """
        This test causes MySQL to return what we consider a temporary local
        error.  We do this by starting MySQL, querying a table, then physically
        removing MySQL's data files.

        This triggers MySQL to return a certain error code which we want to
        consider a temporary local error, which should result in a reconnection
        to MySQL.

        This is arguably the most application-specific behaviour in the txMySQL
        client library.

        """
        res = yield AsyncExecCmds([
            """sh -c '
            cd /var/lib/mysql/foo;
            chmod 0660 *;
            chown mysql:mysql *
            '"""], cmd_prefix="sudo ").getDeferred()
        yield self._start_mysql()
        conn = self._connect_mysql(retry_on_error=True,
            temporary_error_strings=[
                "Can't find file: './foo/foo.frm' (errno: 13)",
            ])
        yield conn.selectDb("foo")
        yield conn.runOperation("create database if not exists foo")
        yield conn.runOperation("create database if not exists foo")
        yield conn.runOperation("drop table if exists foo")
        yield conn.runOperation("create table foo (id int)")
        yield conn.runOperation("insert into foo set id=1")
        result = yield conn.runQuery("select * from foo")
        self.assertEquals(result, [[1]])

        # Now the tricky bit, we have to force MySQL to yield the error message.
        res = yield AsyncExecCmds([
            """sh -c '
            cd /var/lib/mysql/foo;
            chmod 0600 *;
            chown root:root *'
            """], cmd_prefix="sudo ").getDeferred()
        print res
        
        yield conn.runOperation("flush tables") # cause the files to get re-opened
        d = conn.runQuery("select * from foo") # This will spin until we fix the files, so do that pronto
        yield sleep(1)
        res = yield AsyncExecCmds([
            """sh -c '
            cd /var/lib/mysql/foo;
            chmod 0660 *;
            chown mysql:mysql *
            '"""], cmd_prefix="sudo ").getDeferred()
        print res
        result = yield d
        self.assertEquals(result, [[1]])
        conn.disconnect()
   
    @defer.inlineCallbacks
    def test_0700_error_strings_during_connection_phase(self):
        yield self._start_mysql()
        conn = self._connect_mysql(retry_on_error=True,
            temporary_error_strings=[
                "Unknown database 'databasewhichdoesnotexist'",
            ], database='databasewhichdoesnotexist')

        yield conn.runQuery("select * from foo")

    test_0700_error_strings_during_connection_phase.skip = 'Use in debugging, never passes'


    @defer.inlineCallbacks
    def test_0900_autoRepairKeyError(self):
        """
        
        """
        yield AsyncExecCmds(['/opt/HybridCluster/init.d/mysqld stop']).getDeferred()
        sampleBadDataPath = FilePath(__file__).sibling('bad-data')
        target = FilePath('/var/db/mysql/autorepair')
        try:
            target.remove()
        except OSError, e:
            if e.errno != ENOENT:
                raise
        sampleBadDataPath.copyTo(target)
        passwordEntry = pwd.getpwnam('mysql')
        for path in target.walk():
            os.chown(path.path, passwordEntry.pw_uid, passwordEntry.pw_gid)
        yield AsyncExecCmds(['/opt/HybridCluster/init.d/mysqld start']).getDeferred()
        conn = client.MySQLConnection('127.0.0.1', 'root', secrets.MYSQL_ROOT_PASS, 'autorepair',
                                      port=3307, autoRepair=True)
        yield conn.runQuery("select id from mailaliases where username='iceshaman@gmail.com' and deletedate is null")
        conn.disconnect()
    FREEBSD_TESTS.append(test_0900_autoRepairKeyError.__name__)

    # Utility functions:

    def _stop_mysql(self):
        return AsyncExecCmds(['stop mysql'], cmd_prefix='sudo ').getDeferred()
    
    def _start_mysql(self):
        return AsyncExecCmds(['start mysql'], cmd_prefix='sudo ').getDeferred()
    
    def _start_evildaemon(self, secs):
        """
        Simulates a MySQL server which accepts connections but has mysteriously
        stopped returning responses at all, i.e. it's just /dev/null
        """
        return AsyncExecCmds(['python ../test/evildaemon.py %s' % str(secs)], cmd_prefix='sudo ').getDeferred()
    
    def setUp(self):
        """
        Stop MySQL before each test
        """
        name = self._testMethodName
        if onFreeBSD and name not in FREEBSD_TESTS:
            raise unittest.SkipTest("%r only runs on FreeBSD" % (name,))
        elif not onFreeBSD and name in FREEBSD_TESTS:
            raise unittest.SkipTest("%r does not run on FreeBSD" % (name,))
        return self._stop_mysql()

    def tearDown(self):
        """
        Stop MySQL before each test
        """
        reactor.disconnectAll()

    def _connect_mysql(self, **kw):
        if 'database' in kw:
            return client.MySQLConnection('127.0.0.1', 'root', secrets.MYSQL_ROOT_PASS, **kw)
        else:
            return client.MySQLConnection('127.0.0.1', 'root', secrets.MYSQL_ROOT_PASS, 'foo', **kw)


