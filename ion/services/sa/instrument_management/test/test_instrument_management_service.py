#!/usr/bin/env python

"""
@file ion/services/sa/instrument_management/test/test_instrument_management_service.py
@author Ian Katz
@test ion.services.sa.instrument_management.instrument_management_service Unit test suite to cover all service code
"""

from mock import Mock #, sentinel, patch
from pyon.util.unit_test import PyonTestCase
from ion.services.sa.instrument_management.instrument_management_service import InstrumentManagementService
from nose.plugins.attrib import attr

from pyon.util.log import log

#from pyon.core.bootstrap import IonObject

from ion.services.sa.resource_worker_metatest import ResourceWorkerMetatest

from ion.services.sa.instrument_management.instrument_agent_instance_worker import InstrumentAgentInstanceWorker
from ion.services.sa.instrument_management.instrument_agent_worker import InstrumentAgentWorker
from ion.services.sa.instrument_management.instrument_device_worker import InstrumentDeviceWorker
from ion.services.sa.instrument_management.instrument_model_worker import InstrumentModelWorker
from ion.services.sa.instrument_management.platform_agent_instance_worker import PlatformAgentInstanceWorker
from ion.services.sa.instrument_management.platform_agent_worker import PlatformAgentWorker
from ion.services.sa.instrument_management.platform_device_worker import PlatformDeviceWorker
from ion.services.sa.instrument_management.platform_model_worker import PlatformModelWorker
from ion.services.sa.instrument_management.sensor_device_worker import SensorDeviceWorker
from ion.services.sa.instrument_management.sensor_model_worker import SensorModelWorker

#from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
#import unittest

@attr('UNIT', group='sa')
class TestInstrumentManagement(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock('ion.services.sa.instrument_management.instrument_management_service.IonObject')
        #self.mock_ionobj = IonObject
        mock_clients = self._create_service_mock('instrument_management_service')

        self.instrument_mgmt_service = InstrumentManagementService()
        self.instrument_mgmt_service.clients = mock_clients
        
        # must call this manually
        self.instrument_mgmt_service.on_init()

rwm = ResourceWorkerMetatest(TestInstrumentManagement, InstrumentManagementService, log)

rwm.add_resource_worker_unittests(InstrumentAgentInstanceWorker, {"exchange-name": "rhubarb"})
rwm.add_resource_worker_unittests(InstrumentAgentWorker, {"agent_version": "3", "time_source": "the universe"})
rwm.add_resource_worker_unittests(InstrumentDeviceWorker, {"serialnumber": "123", "firmwareversion": "x"})
rwm.add_resource_worker_unittests(InstrumentModelWorker, {"model": "redundant?", "weight": 20000})
rwm.add_resource_worker_unittests(PlatformAgentInstanceWorker, {"exchange-name": "sausage"})
rwm.add_resource_worker_unittests(PlatformAgentWorker, {"tbd": "the big donut"})
rwm.add_resource_worker_unittests(PlatformDeviceWorker, {"serial_number": "2345"})
rwm.add_resource_worker_unittests(PlatformModelWorker, {"tbd": "tammy breathed deeply"})
rwm.add_resource_worker_unittests(SensorDeviceWorker, {"serialnumber": "123"})
rwm.add_resource_worker_unittests(SensorModelWorker, {"model": "redundant field?", "weight": 2})


