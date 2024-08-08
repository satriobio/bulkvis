import panel as pn
import panel.widgets as widgets

from bokeh.plotting import figure
from bokeh.models import RangeSlider
from bokeh.palettes import Category10 

import h5py
import pandas as pd

from siren.utils.squiggletools import BulkFile, SquiggleFile
from siren.modules import base

class Bulkvis(base.Base):
    def __init__(self, file_fn, file_type, group_ids):
        self.df_annotation = None
        self.annotation_widget = pn.widgets.MultiChoice(name='Annotation', value=[], options=[])
        super().__init__(file_fn, file_type, group_ids)

    def _load_data(self, time_range='0-0'):
        super()._load_data(time_range)
        annotation = self.squigglefile.fetch_annotation(f'{self.group_ids[0]}:{time_range}')
        annotation_types = h5py.check_dtype(enum=annotation.dtype['summary_state'])
        annotation_types = {v: k for k, v in annotation_types.items()}
        self.df_annotation = pd.DataFrame(annotation)
        self.df_annotation['summary_state'] = self.df_annotation['summary_state'].map(annotation_types)
        self.annotation_widget.options = list(annotation_types.values())

    def _generate_plot(self):
        super()._generate_plot()

        df_annotation_active = self.df_annotation[self.df_annotation['summary_state'].isin(self.annotation_widget.value)]
        for _, row in df_annotation_active.iterrows():
            self.p1.vspan(x=row['acquisition_raw_index'], line_color='gray', line_width=2)
            
        self.figure = pn.Column(self.p1, self.p2, self.rslider, sizing_mode='stretch_width')

    def _setup_layout(self):
        self.layout = pn.Row(
            pn.Spacer(width=20),
            pn.WidgetBox(
                '''
                # Bulkvis
                ''',
                self.start_time_widget,
                self.end_time_widget,
                self.annotation_widget,
                self.refresh_button
            ),
            pn.Column(
                self.figure,
            )
        )