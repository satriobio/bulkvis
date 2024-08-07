from uuid import uuid4
from io import BytesIO
import h5py
import pod5
from ont_fast5_api.multi_fast5 import MultiFast5File

import pandas as pd

class BulkFile:
    def __init__(self, file_squiggle):
        self.file_squiggle = file_squiggle
        self.protocol, self.ext = self.get_type()
        driver = 'ros3' if self.protocol == 'https' else None
        if self.ext == 'fast5':
            self.file = h5py.File(file_squiggle, "r", driver)
        else:
            raise ValueError(f"Unsupported file type: {self.ext}")

    def get_type(self):
        parts = self.file_squiggle.split(":")
        protocol = parts[0] if len(parts) > 1 else 'local'
        ext = self.file_squiggle.split(".")[-1]
        return protocol, ext

    def list_channels(self):
        return [channel for channel in self.file["Raw"]]

    def fetch_squiggle(self, key):
        channel, start, end = self.parse_position(key)
        self.squiggle = self.file["Raw"][channel]["Signal"][start:end]
        return self.squiggle
    
    def fetch_annotation(self, key):
        freq = 400 
        #HARDCODED
        channel, start, end = self.parse_position(key)
        states = self.file["StateData"][channel]["States"]
        states = states[:]
        states['acquisition_raw_index'] = states['acquisition_raw_index'] / freq
        states['analysis_raw_index']    = states['analysis_raw_index'] / freq
        states['trigger_time']          = states['trigger_time'] / freq
        states = states[(states['acquisition_raw_index'] >= start) & (states['acquisition_raw_index'] <= end)]
        return states

    def fetch_context(self):
        context_tags_group = self.file["UniqueGlobalKey"]["context_tags"]
        context_tags_attrs = dict(context_tags_group.attrs)
        context_tags_attrs = {key: value.decode("utf-8") for key, value in context_tags_attrs.items()}
        return context_tags_attrs
    
    def fetch_tracking(self):
        tracking_id_group = self.file["UniqueGlobalKey"]["tracking_id"]
        tracking_id_attrs = dict(tracking_id_group.attrs)
        tracking_id_attrs = {key: value.decode("utf-8") for key, value in tracking_id_attrs.items()}
        return tracking_id_attrs

    def parse_position(self, key):
        channel, region = key.split(":")
        start, end = map(int, region.split("-"))
        return channel, start, end
    
    def close(self):
        self.file.close()
        self.file = None

class SquiggleFileLegacy:
    def __init__(self, file_squiggle):
        self.file_squiggle = file_squiggle
        self.protocol, self.ext = self.get_type()
        driver = 'ros3' if self.protocol == 'https' else None
        if self.ext == 'fast5':
            self.file = h5py.File(file_squiggle, "r", driver)
        else:
            raise ValueError(f"Unsupported file type: {self.ext}")

    def get_type(self):
        parts = self.file_squiggle.split(":")
        protocol = parts[0] if len(parts) > 1 else 'local'
        ext = self.file_squiggle.split(".")[-1]
        return protocol, ext
    
    def fetch_squiggle(self, key):
        self.squiggle = self.file[key]
        return self.squiggle
    
    def list_reads(self):
        return [read for read in self.file]

class SquiggleFile:
    def __init__(self, file_squiggle):
        self.file_squiggle = file_squiggle
        self.protocol, self.ext = self.get_type()
        driver = 'ros3' if self.protocol == 'https' else None
        if self.ext == 'pod5':
            self.file = pod5.Reader(file_squiggle)
        else:
            raise ValueError(f"Unsupported file type: {self.ext}")

    def get_type(self):
        parts = self.file_squiggle.split(":")
        protocol = parts[0] if len(parts) > 1 else 'local'
        ext = self.file_squiggle.split(".")[-1]
        return protocol, ext
    
    def fetch_squiggle(self, key):
        read = next(self.file.reads(selection=[key]))
        self.squiggle = read.signal
        return self.squiggle

    def list_reads(self):
        return [read_record.read_id for read_record in self.file.reads()]

# def to_fast5(self):
#     fast5_bytes = BytesIO()
#     with MultiFast5File(fast5_bytes, "w") as multi_f5:
#         read0 = multi_f5.create_empty_read(str(uuid4()), "str(self.run_id)")
#         raw_attrs = {
#             "duration": len(self.squiggle),
#             "median_before": 0,
#             "read_id": str(uuid4()),
#             "read_number": 1,
#             "start_mux": 1,
#             "start_time": 400,  # note this is the start time of the read in samples
#             "end_reason": 0,
#         }
#         read0.add_raw_data(self.squiggle, attrs=raw_attrs)
#         # read0.add_channel_info(channel_info)
#         if self.obj_context:
#             read0.add_tracking_id(self.obj_context.tracking_id)
#             read0.add_context_tag(self.obj_context.context_tags)

#     fast5_bytes.seek(0)
#     return fast5_bytes

# def to_pod5(self):
#     pass


# pass through basecaller