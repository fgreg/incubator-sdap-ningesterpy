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
import xarray as xr
import io
import pickle

from sdap.processors.tilereadingprocessor import TileReadingProcessor


class XarrayTilingProcessor(TileReadingProcessor):

    def __init__(self, variable_to_read, latitude, longitude, time, **kwargs):
        super().__init__(variable_to_read, latitude, longitude, **kwargs)

        # Time is required for swath data
        self.time = time

    def read_data(self, tile_specifications, file_path, output_tile):
        ds = xr.open_dataset(file_path, decode_times=True)

        for section_spec, dimtoslice in tile_specifications:

            indexer = {dim: range(s.start, s.stop) for dim, s in dimtoslice.items()}
            tile = ds.isel(**indexer)

            tile_data = io.BytesIO()
            pickle.dump(tile, tile_data, protocol=pickle.HIGHEST_PROTOCOL)
            tile_data_b = tile_data.getvalue()

            output_tile.tile.xarray_data = tile_data_b

            yield output_tile
