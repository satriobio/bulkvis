import numpy as np
import pandas as pd
import plotly.graph_objects as go
import panel as pn
import holoviews as hv
from holoviews import opts
import panel.widgets as widgets

class Plotting:
    def __init__(self, duration=1000, sample_rate=10):
        self.duration = duration
        self.sample_rate = sample_rate
        self.df = None
        self.fig = None
        self.layout = None
        self.start_time = 0
        self.end_time = duration

        self._generate_data()
        self._create_plot()
        self._create_layout()

    def _generate_data(self):
        np.random.seed(0)  # For reproducibility
        time_seconds = np.arange(0, self.duration * self.sample_rate, self.sample_rate)
        values = np.random.normal(loc=0, scale=1, size=len(time_seconds))  # Gaussian noise

        # Create a DataFrame
        self.df = pd.DataFrame({'Time': time_seconds, 'Value': values})

    def _create_plot(self):
        # Filter data based on start and end time
        filtered_df = self.df[(self.df['Time'] >= self.start_time) & (self.df['Time'] <= self.end_time)]
        
        # Create figure
        self.fig = go.Figure()

        # Add trace
        self.fig.add_trace(
            go.Scatter(x=filtered_df['Time'], y=filtered_df['Value'], mode='lines', name='Gaussian Noise')
        )

        # Set title and layout options
        self.fig.update_layout(
            # title_text="Gaussian Noise Time Series",
            xaxis=dict(
                rangeslider=dict(visible=True),
                type="linear"
            )
        )

    def _update_plot(self, event=None):
        # Update plot based on widget values
        self.start_time = self.start_time_widget.value
        self.end_time = self.end_time_widget.value
        self._generate_data()
        self._create_plot()
        self.layout[2] = pn.pane.Plotly(self.fig, sizing_mode='stretch_width')
        
    def _create_layout(self):
        # Create widgets
        self.start_time_widget = widgets.IntInput(name='Start Time', start=0, end=self.duration, step=1, value=self.start_time)
        self.end_time_widget = widgets.IntInput(name='End Time', start=0, end=self.duration, step=1, value=self.end_time)

        # Add a button to refresh plot
        refresh_button = widgets.Button(name='Refresh Plot')
        refresh_button.on_click(self._update_plot)

        # Annotation toggles
        self.vline_annotation_widget = widgets.Checkbox(name='Show Vertical Line Annotations', value=False)

        # Create a callback for annotation toggle
        # self.vline_annotation_widget.param.watch(self._toggle_annotations, 'value')

        multi_choice = pn.widgets.MultiChoice(name='Annotation', value=[],
            options=['Pore', 'Strand', 'Unavailable', 'Unknown'])
        
        # Layout
        self.layout = pn.Row(
            pn.Spacer(width=20),
            # pn.Column(
            pn.WidgetBox(
                '# Bulkvis',
                self.start_time_widget,
                self.end_time_widget,
                multi_choice,
                refresh_button,
                # self.vline_annotation_widget,
                # sizing_mode='stretch_height',
                # width=30
            ),
            pn.pane.Plotly(
                self.fig, 
                sizing_mode='stretch_width'
            )
            # )
        )
    
    def _toggle_annotations(self, event):
        # Show/hide vertical line annotations based on widget state
        if self.vline_annotation_widget.value:
            self.fig.add_vline(x=5, line=dict(color='red', width=2))  # Example annotation
        else:
            self.fig.update_layout(shapes=[s for s in self.fig.layout.shapes if s.type != 'line'])  # Remove annotations