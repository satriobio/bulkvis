import panel as pn
import panel.widgets as widgets

from bokeh.plotting import figure
from bokeh.models import RangeSlider
from bokeh.palettes import Category10 

import h5py
import pandas as pd

from siren.utils.squiggletools import BulkFile, SquiggleFile
from siren.modules import base

class Stingray(base.Base):
    pass

###

# import numpy as np
# import pandas as pd
# import plotly.graph_objects as go
# import panel as pn
# import holoviews as hv
# from holoviews import opts
# import panel.widgets as widgets

# import h5py

# from siren.utils.squiggletools import BulkFile

# class Delete:
#     def __init__(self, file_fn, channel_id):
#         self.bulkfile = BulkFile(file_fn)
#         self.channel_id = channel_id

#         self.df_squiggle = None
#         self.df_annotation = None
#         self.figure = None
#         self.layout = None
#         self.start_time = 0
#         self.end_time = 200
#         self.color_map = {
#             'pore': 'blue',
#             'strand': 'green',
#             'unblocking': 'purple',
#             # Add more states and colors as needed
#         }

#         self.start_time_widget = widgets.IntInput(name='Start Time', start=0, step=1)
#         self.end_time_widget = widgets.IntInput(name='End Time', start=0, step=1)
#         self.annotation_widget = pn.widgets.MultiChoice(name='Annotation', value=[], options=[])
#         self.refresh_button = widgets.Button(name='Refresh Plot')
        
#         self.refresh_button.on_click(self._update_plot)

#         try:
#             self._load_data()
#             self._generate_plot()
#             self._setup_layout()
#             pn.state.notifications.success('Plugin loaded successfully.', duration=2000)
#         except Exception as e:
#             print(f"An error occurred: {e}")
#             pn.state.notifications.warning('Plugin load failed.', duration=2000)

#     def _load_data(self, time_range='0-0'):
#         squiggle = self.bulkfile.fetch_squiggle(f'{self.channel_id}:{time_range}')
#         annotation = self.bulkfile.fetch_annotation(f'{self.channel_id}:{time_range}')
#         self.df_squiggle = pd.DataFrame({'Time': range(len(squiggle)), 'Value': squiggle})

#         annotation_types = h5py.check_dtype(enum=annotation.dtype['summary_state'])
#         annotation_types = {v: k for k, v in annotation_types.items()}
#         self.df_annotation = pd.DataFrame(annotation)
#         self.df_annotation['summary_state'] = self.df_annotation['summary_state'].map(annotation_types)
        
#         self.end_time = len(squiggle)
#         self.annotation_widget.options = list(annotation_types.values())

#     def _generate_plot(self):
#         filtered_data = self.df_squiggle[(self.df_squiggle['Time'] >= self.start_time) & (self.df_squiggle['Time'] <= self.end_time)]

#         self.figure = go.Figure()
#         self.figure.add_trace(
#             go.Scatter(x=filtered_data['Time'], y=filtered_data['Value'], mode='lines', name='Signal')
#         )

#         self.figure.update_layout(
#             xaxis=dict(
#                 rangeslider=dict(visible=True),
#                 type="linear"
#             )
#         )

#         df_active_annotation = self.df_annotation[self.df_annotation['summary_state'].isin(self.annotation_widget.value)]
#         for _, row in df_active_annotation.iterrows():
#             state = row['summary_state']
#             color = self.color_map.get(state, 'grey')
#             x_value = row['acquisition_raw_index']
            
#             self.figure.add_vline(
#                 x=x_value,
#                 line=dict(color=color, width=2)
#             )

#             rg = filtered_data['Value'].max()-filtered_data['Value'].min()

#             self.figure.add_annotation(
#                 x=x_value,
#                 y= filtered_data['Value'].min() + (rg*0.80),  # Adjust the y position as needed
#                 text=state,
#                 showarrow=False,
#                 font=dict(size=12, color='black'),
#                 align='right',
#                 textangle=-90
#                 # bgcolor='white',
#                 # borderpad=4,
#                 # bordercolor='black',
#                 # borderwidth=1
#             )

#     def _update_plot(self, event=None):
#         self.start_time = self.start_time_widget.value
#         self.end_time = self.end_time_widget.value
#         self._load_data(f'{self.start_time}-{self.end_time}')
#         self._generate_plot()
#         self.layout[2] = pn.pane.Plotly(self.figure, sizing_mode='stretch_width')
        
#     def _setup_layout(self):
#         self.layout = pn.Row(
#             pn.Spacer(width=20),
#             pn.WidgetBox(
#                 '''
#                 # Bulkvis
                
#                 ''',
#                 self.start_time_widget,
#                 self.end_time_widget,
#                 self.annotation_widget,
#                 self.refresh_button
#             ),
#             pn.pane.Plotly(
#                 self.figure, 
#                 sizing_mode='stretch_width'
#             )
#         )