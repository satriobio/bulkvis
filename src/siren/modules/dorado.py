import panel as pn
import panel.widgets as widgets

from bokeh.plotting import figure
from bokeh.models import RangeSlider
from bokeh.palettes import Category10 

import h5py
import pandas as pd

from siren.utils.squiggletools import BulkFile, SquiggleFile
from siren.modules import base

class Dorado(base.Base):
    pass

####

# import panel.widgets as widgets
# from pybasecall_client_lib.pyclient import PyBasecallClient
# from pybasecall_client_lib.helper_functions import basecall_with_pybasecall_client
# import panel as pn
# from siren.utils.squiggletools import BulkFile
# import panel.widgets as widgets

# class Delete:
#     def __init__(self):
#         try:
#             options = {
#                 'priority': PyBasecallClient.high_priority,
#                 'client_name': "test_client",
#                 'move_enabled': True 
#             }
#             port_path = "127.0.0.1:5555"
#             client = PyBasecallClient(port_path, 'dna_r9.4.1_e8.1_modbases_5mc_cg_fast')
#             result = client.set_params(options)
#             # result = client.connect()
#             # self._load_data()
#             # self._generate_plot()
#             # self._setup_layout()
#             pn.state.notifications.success('Plugin loaded successfully.', duration=2000)
#         except Exception as e:
#             print(f"An error occurred: {e}")
#             pn.state.notifications.warning('Plugin load failed.', duration=2000)

#         # self.bulkfile = BulkFile(file_fn)
#         # self.channel_id = channel_id

#         # self.df_squiggle = None
#         # self.df_annotation = None
#         # self.figure = None
#         self.layout = None
#         # self.start_time = 0
#         # self.end_time = 200

#         self.start_time_widget = widgets.IntInput(name='Start Time', start=0, step=1)
#         self.end_time_widget = widgets.IntInput(name='End Time', start=0, step=1)
#         # self.annotation_widget = pn.widgets.MultiChoice(name='Annotation', value=[], options=[])
#         self.model_widget = pn.widgets.Select(name='Model', options={'RNA004': 'RNA004'})
#         self.submit_button = widgets.Button(name='Submit')
#         self.export_button = widgets.Button(name='Export')

#         # self.refresh_button.on_click(self._update_plot)

#         # self._load_data()
#         # self._generate_plot()
#         self._setup_layout()
#         pass

#     def _load_data(self, time_range='1-200'):
#         pass

#     def _generate_plot(self):
#         pass

#     def _update_plot(self, event=None):
#         pass

#     def _setup_layout(self):
#         self.layout = pn.Row(
#             pn.Spacer(width=20),
#             pn.WidgetBox(
#                 '''
#                 # Basecall

#                 ''',
#                 self.start_time_widget,
#                 self.end_time_widget,
#                 self.model_widget,
#                 pn.Row(
#                     self.submit_button,
#                     self.export_button,
#                 )
#             ),
#             pn.pane.Markdown(
#                 '''
#                 ```fasta
#                 > Base
#                 ACTGCACGTAGCTAGAC
#                 ```
#                 ''',
#                 sizing_mode='stretch_width'
#             )
#         )