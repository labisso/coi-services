
'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L1_conductivity.py
@description Transforms CTD parsed data into L1 product for conductivity
'''

from pyon.ion.transform import TransformFunction, TransformDataProcess
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log
import numpy as np
from prototype.sci_data.stream_defs import L0_conductivity_stream_definition #, L1_conductivity_stream_definition

from prototype.sci_data.stream_parser import PointSupplementStreamParser
#from prototype.sci_data.constructor_apis import PointSupplementConstructor

### For new granule and stream interface
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule.taxonomy import TaxyTool
from ion.services.dm.utility.granule.granule import build_granule
from pyon.util.containers import get_safe
from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum

class CTDL1ConductivityTransform(TransformFunction):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the conductivity value and scales it according to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.

    '''

    # Make the stream definitions of the transform class attributes... best available option I can think of?
    incoming_stream_def = L0_conductivity_stream_definition()
    #outgoing_stream_def = L1_conductivity_stream_definition()

    def __init__(self):

        ### Parameter dictionaries
        self.defining_parameter_dictionary()


    def defining_parameter_dictionary(self):

        # Define the parameter context objects

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.int64))
        t_ctxt.reference_frame = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 1970-01-01'
        t_ctxt.fill_value = 0x0

        lat_ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=np.float32))
        lat_ctxt.reference_frame = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt.fill_value = 0e0

        lon_ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=np.float32))
        lon_ctxt.reference_frame = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt.fill_value = 0e0

        height_ctxt = ParameterContext('height', param_type=QuantityType(value_encoding=np.float32))
        height_ctxt.reference_frame = AxisTypeEnum.HEIGHT
        height_ctxt.uom = 'meters'
        height_ctxt.fill_value = 0e0

        cond_ctxt = ParameterContext('cond', param_type=QuantityType(value_encoding=np.float32))
        cond_ctxt.uom = 'unknown'
        cond_ctxt.fill_value = 0e0

        data_ctxt = ParameterContext('data', param_type=QuantityType(value_encoding=np.int8))
        data_ctxt.uom = 'byte'
        data_ctxt.fill_value = 0x0

        # Define the parameter dictionary objects

        self.cond = ParameterDictionary()
        self.cond.add_context(t_ctxt)
        self.cond.add_context(lat_ctxt)
        self.cond.add_context(lon_ctxt)
        self.cond.add_context(height_ctxt)
        self.cond.add_context(cond_ctxt)
        self.cond.add_context(data_ctxt)

    def execute(self, granule):
        """Processes incoming data!!!!
        """
        rdt = RecordDictionaryTool.load_from_granule(granule)
        #todo: use only flat dicts for now, may change later...
#        rdt0 = rdt['coordinates']
#        rdt1 = rdt['data']

        conductivity = get_safe(rdt, 'cond') #psd.get_values('conductivity')

        longitude = get_safe(rdt, 'lon') # psd.get_values('longitude')
        latitude = get_safe(rdt, 'lat')  #psd.get_values('latitude')
        time = get_safe(rdt, 'time') # psd.get_values('time')
        height = get_safe(rdt, 'height') # psd.get_values('time')

        log.warn('CTDL1ConductivityTransform: Got conductivity: %s' % str(conductivity))

        root_rdt = RecordDictionaryTool(param_dictionary=self.cond)

        #todo: use only flat dicts for now, may change later...
#        data_rdt = RecordDictionaryTool(taxonomy=self.tx)
#        coord_rdt = RecordDictionaryTool(taxonomy=self.tx)

        scaled_conductivity = conductivity

        for i in xrange(len(conductivity)):
            scaled_conductivity[i] = (conductivity[i] / 100000.0) - 0.5

        root_rdt['cond'] = scaled_conductivity
        root_rdt['time'] = time
        root_rdt['lat'] = latitude
        root_rdt['lon'] = longitude
        root_rdt['height'] = height

#        root_rdt['coordinates'] = coord_rdt
#        root_rdt['data'] = data_rdt

        return build_granule(data_producer_id='ctd_L1_conductivity', param_dictionary=self.cond, record_dictionary=root_rdt)

#        # The L1 conductivity data product algorithm takes the L0 conductivity data product and converts it
#        # into Siemens per meter (S/m)
#        #    SBE 37IM Output Format 0
#        #    1) Standard conversion from 5-character hex string to decimal
#        #    2)Scaling
#        # Use the constructor to put data into a granule
#        psc = PointSupplementConstructor(point_definition=self.outgoing_stream_def, stream_id=self.streams['output'])
#        ### Assumes the config argument for output streams is known and there is only one 'output'.
#        ### the stream id is part of the metadata which much go in each stream granule - this is awkward to do at the
#        ### application level like this!
#
#        for i in xrange(len(conductivity)):
#            scaled_conductivity =  ( conductivity[i] / 100000.0 ) - 0.5
#            point_id = psc.add_point(time=time[i],location=(longitude[i],latitude[i],height[i]))
#            psc.add_scalar_point_coverage(point_id=point_id, coverage_id='conductivity', value=scaled_conductivity)
#
#        return psc.close_stream_granule()



  