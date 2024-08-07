import panel as pn
import holoviews as hv
import hvplot.pandas
import numpy as np
import pandas as pd
import json

from bokeh.plotting import figure, show
from bokeh.models import DateRangeSlider, Spacer, ColumnDataSource, RangeSlider
from bokeh.layouts import column
from bokeh.io import curdoc
from datetime import datetime, time

pn.extension('fontawesome')
pn.extension(notifications=True)

# plot_opts = dict(responsive=True, min_height=400)

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import panel as pn
import holoviews as hv
from holoviews import opts
import panel.widgets as widgets

pn.extension('plotly')

from modules.bulkvis import Plotting

class BulkAnalysisPage:
    def __init__(self, file, channel_id):
        self.file = file
        self.channel_id = channel_id
        self.toolbar = self.create_toolbar()
        self.tabs = pn.Tabs(tabs_location='left', sizing_mode='stretch_both')
        self.get_input('')
        self.layout = self.create_layout()
    
    def get_input(self, event):
        input_json = json.dumps({
            "File Name": self.file,
            "Channel_ID": self.channel_id,
        }, indent=2)
        
        input_content = pn.pane.JSON(json.loads(input_json), name='Metadata', theme='light')
        self.tabs.append(('Input', input_content))
    
    def fetch_metadata(self, event):
        metadata_json = json.dumps({
            "Meta Data": "Meta Data",
        }, indent=2)
        
        metadata_content = pn.pane.JSON(json.loads(metadata_json), name='Metadata', theme='light')
        self.tabs.append(('Metadata', metadata_content))
    
    def generate_plot(self, event):
        plot = Plotting()
        plot_content = plot.layout
        self.tabs.append(("Bulkvis", plot_content))

    def basecall(self, event):
        bascall_content = pn.pane.Markdown('\>Basecall\nACGTCACGCTCGTCGC')
        self.tabs.append((f'Basecall', bascall_content))

    def find_tail(self, event):
        plot = Plotting()
        plot_content = plot.layout
        self.tabs.append(("Stingray", plot_content))

    def find_motif(self, event):
        plot = Plotting()
        plot_content = plot.layout
        self.tabs.append(("Motif", plot_content))
    
    def find_split(self, event):
        plot = Plotting()
        plot_content = plot.layout
        self.tabs.append(("Split", plot_content))

    def export_fast5(self, event):
        pass

    def export_pod5(self, event):
        pass
    
    def create_toolbar(self):
        # Create toolbar with buttons and icons
        btn_file        = pn.widgets.Button(name='', button_type='primary', icon='file')
        btn_metadata    = pn.widgets.Button(name='', button_type='primary', icon='tag')
        btn_plot        = pn.widgets.Button(name='', button_type='primary', icon='chart-line')
        btn_basecall    = pn.widgets.Button(name='', button_type='primary', icon='letter-case-upper')
        btn_tail        = pn.widgets.Button(name='', button_type='primary', icon='ruler-3')
        btn_motif       = pn.widgets.Button(name='', button_type='primary', icon='fingerprint')
        btn_split       = pn.widgets.Button(name='', button_type='primary', icon='line-dashed')
        btn_export      = pn.widgets.Button(name='', button_type='primary', icon='download')
        
        # Set up event handlers
        btn_file.on_click(self.get_input)
        btn_metadata.on_click(self.fetch_metadata)
        btn_plot.on_click(self.generate_plot)
        btn_basecall.on_click(self.basecall)
        btn_tail.on_click(self.find_tail)
        btn_motif.on_click(self.find_motif)
        btn_split.on_click(self.find_split)
        btn_export.on_click(self.export_pod5)
        
        toolbar = pn.Column(
            btn_file,
            btn_metadata,
            btn_plot, 
            btn_basecall,
            btn_tail,
            btn_motif,
            btn_split,
            btn_export
        )
        return toolbar

    # def create_plot(self):
        # Simulate creating a plot based on the read ID
        # plot = Plotting()
        # return plot.layout
        # x = np.linspace(0, 10, 100)
        # y = np.sin(x) if 'sine' in self.read_id else np.cos(x)
        # df = pd.DataFrame({'x': x, 'y': y})
        # plot = df.hvplot(x='x', y='y', title=f'Plot').opts(axiswise=True)
        # return plot

    # def generate_plot(self, event):
    #     new_plot = self.create_plot()
    #     self.tabs.append((f'Plot', new_plot))

    # def bases(self, event):
    #     fasta = pn.pane.Markdown('\>Basecall\nACGTCACGCTCGTCGC')
    #     self.tabs.append((f'Basecall', fasta))

    def create_layout(self):
        layout = pn.Column(
            pn.Row(
                pn.Spacer(height=20),
            ),
            pn.Row(
                # pn.Spacer(width=30),
                pn.Column(self.tabs),  # Tab section
                pn.Column(self.toolbar, width=50),
            )
        )

        return layout

class SquiggleAnalysisPage:
    def __init__(self, read_id):
        self.read_id = read_id
        # self.metadata = 
        self.toolbar = self.create_toolbar()
        self.tabs = pn.Tabs(tabs_location='left', sizing_mode='stretch_both')
        self.tabs.append(('Metadata',self.create_metadata()))
        self.layout = self.create_layout()
    
    def create_metadata(self):
        # Simulate fetching metadata as a JSON object
        metadata_json = json.dumps({
            "Read ID": self.read_id,
            "Length": 15000,
            "Quality": "High",
            "Sample Date": "2023-07-10",
            "Notes": "Sample data for demonstration purposes."
        }, indent=2)
        
        metadata_section = pn.pane.JSON(json.loads(metadata_json), name='Metadata', theme='light')
        return metadata_section
    
    def create_toolbar(self):
        # Create toolbar with buttons and icons
        generate_plot_button = pn.widgets.Button(name='', button_type='primary', icon='chart-line')
        basecall_button = pn.widgets.Button(name='', button_type='primary', icon='letter-case-upper')
        export_button = pn.widgets.Button(name='', button_type='primary', icon='download')
        
        # Set up event handlers
        generate_plot_button.on_click(self.generate_plot)
        basecall_button.on_click(self.bases)
        
        toolbar = pn.Column(generate_plot_button, basecall_button, export_button)
        return toolbar

    def create_plot(self):
        # Simulate creating a plot based on the read ID
        x = np.linspace(0, 10, 100)
        y = np.sin(x) if 'sine' in self.read_id else np.cos(x)
        df = pd.DataFrame({'x': x, 'y': y})
        plot = df.hvplot(x='x', y='y', title=f'Plot').opts(axiswise=True)
        return plot

    def generate_plot(self, event):
        new_plot = self.create_plot()
        self.tabs.append((f'Plot', new_plot))

    def bases(self, event):
        fasta = pn.pane.Markdown('\>Basecall\nACGTCACGCTCGTCGC')
        self.tabs.append((f'Basecall', fasta))

    def create_layout(self):
        layout = pn.Column(
            pn.Row(
                pn.Spacer(height=20),
            ),
            pn.Row(
                # pn.Spacer(width=30),
                pn.Column(self.tabs),  # Tab section
                pn.Column(self.toolbar, width=50,),
            )
        )

        return layout

class PlotApp:
    def __init__(self):
        self.file_uri = None
        self.all_channel_ids = []

        # Initialize widgets
        self.load_squiggle_button = pn.widgets.Button(name='Load Squiggle', button_type='primary', sizing_mode='stretch_width')
        self.channel_filter_input = pn.widgets.TextInput(name='Filter Channel', placeholder='Enter a string here...', sizing_mode='stretch_width', visible=False)
        self.channel_ids_select = pn.widgets.Select(name='Channel IDs', options=[], size=10, sizing_mode='stretch_width', visible=False)
        self.btn_bulk_analysis = pn.widgets.Button(name='Open Notebook', button_type='primary', icon='notebook', visible=False)

        self.read_filter_input = pn.widgets.TextInput(name='Filter Read ID', placeholder='Enter a string here...', sizing_mode='stretch_width', visible=False)
        self.read_ids_multiselect = pn.widgets.MultiSelect(name='Read IDs', options=[], size=10, sizing_mode='stretch_width', visible=False)
        self.btn_squiggle_analysis = pn.widgets.Button(name='Open Notebook', button_type='primary', icon='notebook', visible=False)

        # Set up event watchers
        self.channel_filter_input.param.watch(self.filter_channel_ids, 'value')
        self.btn_bulk_analysis.on_click(self.add_bulk_analysis)
        self.btn_squiggle_analysis.on_click(self.add_squiggle_analysis)
        self.load_squiggle_button.on_click(self.show_modal)

        self.msg = pn.pane.Markdown("No data available")

        # Set up sidebar and main template
        self.sidebar = pn.Column(
            self.load_squiggle_button,
            self.msg, 
            self.channel_filter_input, 
            self.channel_ids_select,
            self.btn_bulk_analysis,
            self.read_filter_input, 
            self.read_ids_multiselect, 
            self.btn_squiggle_analysis
        )
        self.template = pn.template.MaterialTemplate(title='Siren', sidebar=self.sidebar)

        # Set up tabs
        self.tabs = pn.Tabs(closable=True, sizing_mode='stretch_width')
        self.template.main.append(self.tabs)
        self.tabs.append(('Welcome', self.page_welcome()))
        self.tabs.append(('About', self.page_about()))

        # Set up modal content
        self.modal_content = pn.Column(
            pn.pane.Markdown("# Select Squiggle"),
            pn.widgets.TextInput(name='Local path/S3 URI', placeholder='Enter Local path or S3 URI here...', sizing_mode='stretch_width'),
            pn.widgets.Button(name='Load Data', button_type='primary'),
            width=400
        )
        self.modal_content[1].param.watch(self.set_value, 'value')
        self.modal_content[-1].on_click(self.load_data)
        self.template.modal.append(self.modal_content)

    def page_welcome(self):
        welcome_text = """
        ## Welcome to Siren
        This application allows you to load and analyze squiggle data.
        """
        return pn.pane.Markdown(welcome_text)

    def page_about(self):
        about_text = """
        ## About Siren
        Siren is an application designed to help you analyze and visualize squiggle data.
        """
        return pn.pane.Markdown(about_text)

    def toggle_data_source_input(self, event):
        if event.new == 'Local':
            self.modal_content[2].visible = True
            self.modal_content[3].visible = False
        else:
            self.modal_content[2].visible = False
            self.modal_content[3].visible = True

    def toggle_select_input(self, data_type):
        self.msg.visible = False
        if data_type == 'bulk':
            self.channel_filter_input.visible = True
            self.channel_ids_select.visible = True
            self.btn_bulk_analysis.visible = True
            self.read_filter_input.visible = False
            self.read_ids_multiselect.visible = False
            self.btn_squiggle_analysis.visible = False
        else:
            self.channel_filter_input.visible = False
            self.channel_ids_select.visible = False
            self.btn_bulk_analysis.visible = False
            self.read_filter_input.visible = True
            self.read_ids_multiselect.visible = True
            self.btn_squiggle_analysis.visible = True

    def show_modal(self, event):
        self.template.open_modal()

    def load_read_ids(self, data_source=None):
        if data_source:
            return [f'Read {i}' for i in range(1, 11)]  # Simulated read IDs
        return []

    def filter_channel_ids(self, event):
        try:
            filter_text = event.new.lower()
            if not filter_text:
                filtered_options = self.all_channel_ids
            else:
                filtered_options = [option for option in self.all_channel_ids if filter_text in option.lower()]
            self.channel_ids_select.options = filtered_options if filtered_options else []
        except AttributeError as e:
            print(f"An error occurred: {e}")

    def set_value(self, event):
        self.file_uri = event.new

    def add_bulk_analysis(self, event):
        read_id = self.channel_ids_select.value
        if read_id:
            analysis_page = BulkAnalysisPage(self.modal_content[1].value, read_id)  # Assuming BulkAnalysisPage is defined elsewhere
            self.tabs.append((f'Analysis for {read_id}', analysis_page.layout))

    def add_squiggle_analysis(self, event):
        read_id = self.read_ids_multiselect.value
        if read_id:
            analysis_page = SquiggleAnalysisPage(read_id)  # Assuming SquiggleAnalysisPage is defined elsewhere
            self.tabs.append((f'Analysis for {read_id}', analysis_page.layout))

    def get_type(self, uri):
        return uri.split('.')[-1]

    def load_data(self, event):
        self.template.close_modal()
        try:
            self.data_type = self.get_type(self.file_uri)  # Assuming check_type is defined elsewhere
            if self.data_type == 'fast5':
                self.toggle_select_input('bulk')
                self.channel_ids_select.options = self.load_read_ids(data_source=True)
                self.all_channel_ids = self.channel_ids_select.options
            else:
                self.toggle_select_input('non-bulk')
                self.read_ids_multiselect.options = self.load_read_ids(data_source=True)
            pn.state.notifications.success('Data loaded successfully.', duration=2000)
        except Exception as e:
            print(f"An error occurred: {e}")
            pn.state.notifications.warning('Data load failed.', duration=2000)

    def servable(self):
        return self.template.servable()

# Custom CSS
raw_css = """
.bk-header { width: 100px; }
.mdc-dialog__content { background-color: white; }
"""

pn.extension(raw_css=[raw_css])

# Instantiate and serve the app
app = PlotApp()
app.servable()