import panel as pn
import panel.widgets as widgets
import plotly.graph_objects as go

import h5py
import pandas as pd

class Template:
    def __init__(self, file_fn, channel_id):
        try:
            self.bulkfile = BulkFile(file_fn)
            self.channel_id = channel_id

            self.df_squiggle = None
            self.figure = None
            self.layout = None
            self.start_time = 0
            self.end_time = 200

            self.start_time_widget = widgets.IntInput(name='Start Time', start=0, step=1)
            self.end_time_widget = widgets.IntInput(name='End Time', start=0, step=1)
            self.refresh_button = widgets.Button(name='Refresh')
    
            self.refresh_button.on_click(self._update_plot)

            self._load_data()
            self._generate_plot()
            self._setup_layout()
            pn.state.notifications.success('Plugin loaded successfully.', duration=2000)
        except Exception as e:
            print(f"An error occurred: {e}")
            pn.state.notifications.warning('Plugin load failed.', duration=2000)

    def _load_data(self, time_range='0-0'):
        squiggle = self.bulkfile.fetch_squiggle(f'{self.channel_id}:{time_range}')
        annotation = self.bulkfile.fetch_annotation(f'{self.channel_id}:{time_range}')
        self.df_squiggle = pd.DataFrame({'Time': range(len(squiggle)), 'Value': squiggle})

        annotation_types = h5py.check_dtype(enum=annotation.dtype['summary_state'])
        annotation_types = {v: k for k, v in annotation_types.items()}
        self.df_annotation = pd.DataFrame(annotation)
        self.df_annotation['summary_state'] = self.df_annotation['summary_state'].map(annotation_types)
        
        self.end_time = len(squiggle)
        self.annotation_widget.options = list(annotation_types.values())

    def _generate_plot(self):
        filtered_data = self.df_squiggle[(self.df_squiggle['Time'] >= self.start_time) & (self.df_squiggle['Time'] <= self.end_time)]

        self.figure = go.Figure()
        self.figure.add_trace(
            go.Scatter(x=filtered_data['Time'], y=filtered_data['Value'], mode='lines', name='Signal')
        )

        self.figure.update_layout(
            xaxis=dict(
                rangeslider=dict(visible=True),
                type="linear"
            )
        )

    def _update_plot(self, event=None):
        self.start_time = self.start_time_widget.value
        self.end_time = self.end_time_widget.value
        self._load_data(f'{self.start_time}-{self.end_time}')
        self._generate_plot()
        self.layout[2] = pn.pane.Plotly(self.figure, sizing_mode='stretch_width')
        
    def _setup_layout(self):
        self.layout = pn.Row(
            pn.Spacer(width=20),
            pn.WidgetBox(
                '''
                # Template
                ''',
                self.start_time_widget,
                self.end_time_widget,
                self.refresh_button
            ),
            pn.pane.Plotly(
                self.figure, 
                sizing_mode='stretch_width'
            )
        )