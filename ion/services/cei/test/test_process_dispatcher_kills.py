import os
import uuid
import unittest
import logging
import time
import signal

from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest

from pyon.util.int_test import IonIntegrationTestCase
from pyon.core import bootstrap

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition, ProcessSchedule, ProcessTarget,\
    ProcessStateEnum, ProcessQueueingMode, ProcessRestartMode, ProcessDefinitionType

from ion.services.cei.test import ProcessStateWaiter

try:
    from epuharness.fixture import TestFixture
except ImportError:
    raise SkipTest("epuharness not available.")

from epu.test import ZooKeeperTestMixin

log = logging.getLogger(__name__)


pd_deployment = """
pyon-process-dispatchers:
  process_dispatcher:
    config:
      dashi:
        sysname: %(sysname)s_cei

      server:
        zookeeper:
          hosts: %(zk_hosts)s
          path: %(zk_path)s
          timeout: %(zk_timeout)s
      system:
        name: %(sysname)s
      processdispatcher:
        monitor_resource_heartbeats: false
        dashi_messaging: true
        dashi_uri: amqp://guest:guest@localhost/
        dashi_exchange: xchng
        static_resources: true
        backend: native
        heartbeat_queue: processdispatcher_heartbeats
        engines:
          engine1:
            slots: 100
            base_need: 1
            heartbeat_period: 2
            heartbeat_warning: 4
            heartbeat_missing: 6
        default_engine: engine1
      pyon_directory: %(pyon_path)s
      replica_count: %(pd_replica_count)s
pyon-nodes:
  node0:
    engine: engine1
    process-dispatcher: process_dispatcher

    eeagents:
      node0_eeagent0:
        config:
          agent:
            resource_id: node0_eeagent0
          eeagent:
            heartbeat: 2
            heartbeat_queue: processdispatcher_heartbeats
            launch_type:
              name: pyon
              pyon_directory: %(pyon_path)s
              persistence_directory: %(persistence_dir)s.node0.node0_eeagent0
            name: node0_eeagent0
            slots: 200
          system:
            name: %(sysname)s

      node0_eeagent1:
        config:
          agent:
            resource_id: node0_eeagent1
          eeagent:
            heartbeat: 2
            heartbeat_queue: processdispatcher_heartbeats
            launch_type:
              name: pyon
              pyon_directory: %(pyon_path)s
              persistence_directory: %(persistence_dir)s.node0.node0_eeagent1
            name: node0_eeagent1
            slots: 200
          system:
            name: %(sysname)s

      node0_eeagent2:
        config:
          agent:
            resource_id: node0_eeagent2
          eeagent:
            heartbeat: 2
            heartbeat_queue: processdispatcher_heartbeats
            launch_type:
              name: pyon
              pyon_directory: %(pyon_path)s
              persistence_directory: %(persistence_dir)s.node0.node0_eeagent2
            name: node0_eeagent2
            slots: 200
          system:
            name: %(sysname)s

dashi_sysname: %(sysname)s_cei
rabbitmq_exchange: xchng
server:
  amqp:
    exchange: xchng
    host: localhost
    password: guest
    username: guest
"""


class KillEvents(object):
    BETWEEN_PROCESS_SCHEDULES = 0
    AFTER_RUNNING = 1
    BEFORE_TERMINATES = 2
    AFTER_TERMINATES = 3


@unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
@attr('LOCOINT', group='cei')
class BasePDKillsFixture(IonIntegrationTestCase, TestFixture, ZooKeeperTestMixin):
    use_zk_proxy = False
    zk_timeout = 5
    pd_replica_count = 1

    ZK_BASE = "/PDKillTests"

    def setUp(self):
        self.setup_zookeeper(self.ZK_BASE, use_proxy=self.use_zk_proxy)
        self.addCleanup(self.cleanup_zookeeper)

        self.sysname = bootstrap.get_sys_name()
        self.dashi_sysname = self.sysname + "_cei"

        self.deployment = pd_deployment % dict(zk_hosts=self.zk_hosts, zk_timeout=self.zk_timeout,
            zk_path=self.zk_base_path, pd_replica_count=self.pd_replica_count,
            sysname=self.sysname, pyon_path=os.getcwd(), persistence_dir='/tmp/cei_persist')

        self.setup_harness(exchange="xchng", sysname=self.dashi_sysname)
        self.addCleanup(self.cleanup_harness)

        self.epuharness.start(deployment_str=self.deployment)

        # also start a local container for client messaging
        self._start_container()
        self.addCleanup(self._stop_container)

        self.pd_cli = ProcessDispatcherServiceClient(node=self.container.node)

        self.process_definition = ProcessDefinition(name='test_process')
        self.process_definition.executable = {'module': 'ion.services.cei.test.test_process_dispatcher',
                                              'class': 'TestProcess'}
        self.process_definition_id = self.pd_cli.create_process_definition(self.process_definition)

        self.waiter = None

    def tearDown(self):
        if self.waiter:
            self.waiter.stop()

    def launch_watch_cancel_procs(self, event_callback, count, sleep=0):

        self.waiter = ProcessStateWaiter()
        self.waiter.start()

        process_schedule = ProcessSchedule()
        process_schedule.queueing_mode = ProcessQueueingMode.ALWAYS
        pids = []
        for i in range(count):
            pid = self.pd_cli.create_process(self.process_definition_id)
            self.pd_cli.schedule_process(self.process_definition_id,
                process_schedule, configuration={}, process_id=pid)
            pids.append(pid)

            event_callback(KillEvents.BETWEEN_PROCESS_SCHEDULES, index=i)

        self.waiter.await_many_state_events(pids, ProcessStateEnum.RUNNING, timeout=60)

        event_callback(KillEvents.AFTER_RUNNING)

        if sleep:
            time.sleep(sleep)

        event_callback(KillEvents.BEFORE_TERMINATES)

        for pid in pids:
            self.pd_cli.cancel_process(pid)

        event_callback(KillEvents.AFTER_TERMINATES)
        self.waiter.await_many_state_events(pids, ProcessStateEnum.TERMINATED, timeout=60)


class PDKillsTests(BasePDKillsFixture):

    # run through the workflow without any kills (no-op callback)
    def test_basic(self):
        def noop(*args, **kwargs):
            pass

        self.launch_watch_cancel_procs(noop, 100)
