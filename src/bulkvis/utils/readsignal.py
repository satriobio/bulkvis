from io import BytesIO
from uuid import uuid4
import math

import numpy as np
import pandas as pd

import h5py
import pod5 as p5
from ont_fast5_api.multi_fast5 import MultiFast5File

class ReadSignal():
    """
    :param signal_file:
    :return:

    """
    def __init__(self):
        self.is_bulk = False
        self.signal = []
        self.obj_context = None

    def read_signal(self, signal_file, key):
        self.file_signal = signal_file
        self.key = key
        file_extension = self.file_signal.split(".")[-1]
        protocol = self.file_signal.split(":")[0]

        if file_extension == 'fast5':
            if protocol == "https" or protocol == "s3":
                driver = "ros3"
            else:
                driver = None

            self.f = h5py.File(self.file_signal, "r", driver=driver)

            # TO DO: check bulk
            # TO DO: get freq and rate
            self.is_bulk = True
            self.freq = 4000
            self.rate = 30

            self.channel, self.start, self.end = parse_position(key)

            if self.is_bulk:
                start = math.floor(self.start * self.freq)
                end = math.floor(self.end * self.freq)
                self.signal = self.f["Raw"][self.channel]["Signal"][start:end]
                self.df_annotation, self.df_annotation_label = self.get_annotation()
                self.obj_context = self.get_context()
            else:
                self.signal = self.f[self.key]
                self.df_annotation = None
                self.df_annotation_label = None
                self.obj_context = None

        if file_extension == 'pod5':
            self.is_bulk = False
            # TO DO: handle s3

            self.f = p5.Reader(self.file_signal)

            with self.f as reader:
                read = next(reader.reads(selection=[self.key]))
                self.signal = read.signal
                self.df_annotation = None
                self.df_annotation_label = None
                self.obj_context = None

    def get_annotation(self):
        if self.is_bulk:
            states = self.f["StateData"][self.channel]["States"]
            fields = ["acquisition_raw_index", "summary_state"]
            enum_field = "summary_state"

            data_labels = {}
            for field in fields:
                data_labels[field] = states[field]

            data_dtypes = {}
            if h5py.check_dtype(enum=states.dtype[enum_field]):
                dataset_dtype = h5py.check_dtype(enum=states.dtype[enum_field])
                data_dtypes = {v: k for k, v in dataset_dtype.items()}

            df_annotation = pd.DataFrame(data=data_labels)
            df_annotation.rename(columns={"acquisition_raw_index": "Time"}, inplace=True)
            df_annotation["Time"] = df_annotation["Time"] / self.freq
            df_annotation["state"] = df_annotation["summary_state"].map(data_dtypes)
            df_annotation = df_annotation[(df_annotation["Time"] >= self.start) & (df_annotation["Time"] <= self.end)]

            df_annotation_label = pd.DataFrame(list(data_dtypes.items()), columns=["Key", "Value"])

            return df_annotation, df_annotation_label
    
    def get_context(self):
        context_tags_group = self.f["UniqueGlobalKey"]["context_tags"]
        context_tags_attrs = dict(context_tags_group.attrs)
        context_tags_attrs = {key: value.decode("utf-8") for key, value in context_tags_attrs.items()}

        tracking_id_group = self.f["UniqueGlobalKey"]["tracking_id"]
        tracking_id_attrs = dict(tracking_id_group.attrs)
        tracking_id_attrs = {key: value.decode("utf-8") for key, value in tracking_id_attrs.items()}

        return {"context_tags": context_tags_attrs, "tracking_id": tracking_id_attrs}
    
    def get_df(self):
        if self.is_bulk:
            start = math.floor(self.start * self.freq)
            end = math.floor(self.end * self.freq)
            time = np.arange(start, end)
            df = pd.DataFrame({"Time": time, "Signal": self.signal})
            df = df[::self.rate]
            df["Time"] = df["Time"] / self.freq

            return df
        else:
            time = np.arange(0, len(self.signal))
            len(time)
            len(self.signal)
            df = pd.DataFrame({"Time": time, "Signal": self.signal})

            return df
        
    def get_fast5(self):
        fast5_bytes = BytesIO()
        with MultiFast5File(fast5_bytes, "w") as multi_f5:
            read0 = multi_f5.create_empty_read(str(uuid4()), "str(self.run_id)")
            raw_attrs = {
                "duration": len(self.signal),
                "median_before": 0,
                "read_id": str(uuid4()),
                "read_number": 1,
                "start_mux": 1,
                "start_time": 400,  # note this is the start time of the read in samples
                "end_reason": 0,
            }
            read0.add_raw_data(self.signal, attrs=raw_attrs)
            # read0.add_channel_info(channel_info)
            if self.obj_context:
                read0.add_tracking_id(self.obj_context.tracking_id)
                read0.add_context_tag(self.obj_context.context_tags)

        fast5_bytes.seek(0)
        return fast5_bytes

def parse_position(key):
        channel = f"Channel_{key.split(':')[0]}"

        start = int(key.split(":")[1].split("-")[0])
        end = int(key.split(":")[1].split("-")[1])
        
        return channel, start, end