# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import unittest

import nexusproto.DataTile_pb2

from sdap.processors import xarraytiler


class TestTimeSeriesReader(unittest.TestCase):

    def test_wswm_data(self):
        output_tile = nexusproto.DataTile_pb2.NexusTile()
        slices = {
            'time': slice(0, 5832),
            'rivid': slice(2572, 2573)
        }

        processor = xarraytiler.XarrayTilingProcessor("Qout", "lat", "lon", "time")

        data = processor.read_data([("time:0:5832;rivid:24520423:24520424", slices)],
                                   "/Users/greguska/data/swot_example/latest/Qout_WSWM_729days_p0_dtR900s_n1_preonly_20160416.nc",
                                   output_tile)

        print(next(data))
