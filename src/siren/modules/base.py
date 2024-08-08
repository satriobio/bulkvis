import panel as pn
import panel.widgets as widgets

from bokeh.plotting import figure
from bokeh.models import RangeSlider
from bokeh.palettes import Category10 
# import plotly.graph_objects as go

import h5py
import pandas as pd

from siren.utils.squiggletools import BulkFile, SquiggleFile

class Base:
    def __init__(self, file_fn, file_type, group_ids):
        self.file_type = file_type
        if self.file_type == 'fast5':
            self.squigglefile = BulkFile(file_fn)
        else:
            self.squigglefile = SquiggleFile(file_fn)

        self.group_ids = group_ids

        self.df_squiggle = None
        self.p1 = None
        self.p2 = None
        self.rslider = None
        self.figure = None
        self.layout = None
        self.start_time = 0
        self.end_time = 200

        self.start_time_widget = widgets.IntInput(name='Start Time', start=0, step=1)
        self.end_time_widget = widgets.IntInput(name='End Time', start=0, step=1)
        self.refresh_button = widgets.Button(name='Refresh')
        self.refresh_button.on_click(self._update_plot)
        self.tabs = pn.Tabs(tabs_location='above', sizing_mode='stretch_both')

        try:
            self._load_data()
            self._generate_plot()
            self._setup_layout()
            # pn.state.notifications.success('Plugin loaded successfully.', duration=2000)
        except Exception as e:
            print(f"An error occurred: {e}")
            # pn.state.notifications.warning('Plugin load failed.', duration=2000)

    def _load_data(self, time_range='0-0'):

        self.df_squiggle = []
        for group_id in self.group_ids:
            if self.file_type == 'fast5':
                squiggle = self.squigglefile.fetch_squiggle(f'{group_id}:{time_range}')
            else:
                squiggle = self.squigglefile.fetch_squiggle(f'{group_id}')
            df_squiggle = pd.DataFrame({'Time': range(len(squiggle)), 'Value': squiggle})
            self.df_squiggle.append(df_squiggle)

    def _generate_plot(self):
        start = self.start_time
        end = self.end_time

        self.p1 = figure(min_border_left=50, min_border_right=50, tools='pan,wheel_zoom,box_zoom,reset', 
                         x_range=(start, end), sizing_mode='stretch_width')
        self.p1.toolbar.logo = None

        colors = Category10[10] 
        filtered_data_list = []
        for df_squiggle in self.df_squiggle:
            filtered_data = df_squiggle[(df_squiggle['Time'] >= start) & (df_squiggle['Time'] <= end)]
            filtered_data_list.append(filtered_data)
        
        for idx, filtered_data in enumerate(filtered_data_list):
            self.p1.line('Time', 'Value', source=filtered_data, color=colors[idx % len(colors)])

        self.p2 = figure(min_border_left=50, min_border_right=50, height=80, toolbar_location=None, 
                         tools="", sizing_mode='stretch_width')
        self.p2.toolbar.logo = None
        self.p2.toolbar.active_drag = None
        self.p2.toolbar.active_scroll = None
        self.p2.toolbar.active_tap = None
        self.p2.yaxis.major_label_text_color = None
        self.p2.yaxis.major_tick_line_color = None
        self.p2.yaxis.minor_tick_line_color = None
        self.p2.grid.grid_line_color = None

        for idx, filtered_data in enumerate(filtered_data_list):
            self.p2.line('Time', 'Value', source=filtered_data, color=colors[idx % len(colors)])

        self.rslider = RangeSlider(margin=50, start=start, end=end, value=(start, end), title=None, show_value=False, 
                                   sizing_mode='stretch_width')
        self.rslider.js_link('value', self.p1.x_range, 'start', attr_selector=0)
        self.rslider.js_link('value', self.p1.x_range, 'end', attr_selector=1)

        self.figure = pn.Column(self.p1, self.p2, self.rslider, sizing_mode='stretch_width')

    # # PLOTLY EASIER TO WORK WITH, BUT HEAVY
    # def _generate_plot(self):
    #     self.figure = go.Figure()
    
    #     for df_squiggle in self.df_squiggle:
    #         filtered_data = df_squiggle[(df_squiggle['Time'] >= self.start_time) & (df_squiggle['Time'] <= self.end_time)]
    
    #         self.figure.add_trace(
    #             go.Scatter(x=filtered_data['Time'], y=filtered_data['Value'], mode='lines', name='Signal')
    #         )
    
    #     self.figure.update_layout(
    #         xaxis=dict(
    #             rangeslider=dict(visible=True),
    #             type="linear"
    #         )
    #     )

    def _update_plot(self, event=None):
        self.start_time = self.start_time_widget.value
        self.end_time = self.end_time_widget.value
        self._load_data(f'{self.start_time}-{self.end_time}')
        self._generate_plot()
        self.layout[2] = pn.Column(self.figure) 

    def _setup_layout(self):
        # if len(self.tabs) > 0:
        #     self.tabs[0] = pn.Column(self.figure)
        # else:
        #     self.tabs.append(pn.Column(self.figure))

        self.layout = pn.Row(
            pn.Spacer(width=20),
            pn.WidgetBox(
                '''
                # Base Visualization
                ''',
                self.start_time_widget,
                self.end_time_widget,
                self.refresh_button
            ),
            # self.tabs
            pn.Column(
                self.figure,
            ) 
            # pn.pane.Plotly(
            #     self.figure, 
            #     sizing_mode='stretch_width'
            # )
        )