from siren.modules import base, bulkvis
import panel as pn
# import holoviews as hv
# import hvplot.pandas
import numpy as np
import pandas as pd
import json

from bokeh.plotting import figure, show
from bokeh.models import DateRangeSlider, Spacer, ColumnDataSource, RangeSlider
from bokeh.layouts import column
from bokeh.io import curdoc
from datetime import datetime, time

# pn.extension('fontawesome')
pn.extension(notifications=True)

# plot_opts = dict(responsive=True, min_height=400)

import numpy as np
import pandas as pd
# import plotly.graph_objects as go
import panel as pn
# import holoviews as hv
# from holoviews import opts
import panel.widgets as widgets

pn.extension('plotly')

# from siren.mod import base
from siren import mod as base
from siren.modules import dorado
from siren.utils.squiggletools import BulkFile, SquiggleFile


class AppMod:
    pass

class FileSet:
    def __init__(self):
        self._setup_layout()
        pass

    def _setup_layout(self):

        self.layout = pn.Row(
            
        )

class Siren:
    def __init__(self):
        self.signal_input = pn.widgets.TextInput(name='Signal data path/S3 URI', placeholder='Path or S3 URI...')
        self.bulk_input = pn.widgets.TextInput(name='Bulk signal path/S3 URI', placeholder='Path or S3 URI...')
        self.aln_input = pn.widgets.TextInput(name='Alignment signal path/S3 URI', placeholder='Path or S3 URI...')
        
        self.load_dataset_button = pn.widgets.Button(name='Load dataset', button_type='primary', sizing_mode='stretch_width')
        self.load_dataset_button.on_click(self.load_data)
        

        self.load_example_button = pn.widgets.Button(name='Load example', button_type='default', sizing_mode='stretch_width')
        # self.load_example_button.on_click(self.load_data)

        self.file_signal = None
        self.file_signal_bulk = None
        self.file_aln = None

        self.sidebar = pn.Column(
            pn.pane.Markdown(
                """
                # Inputs
                ## Signal Data

                Supported formats: POD5, FAST5 (legacy support).
                """
            ),
            self.signal_input,

            pn.pane.Markdown(
                """
                ## Bulk Signal Data

                Supported format: FAST5.
                """
            ),
            self.bulk_input,

            pn.pane.Markdown(
                """
                ## Alignment Data (Optional)

                Aligned reads in BAM format. Index file located in the same directory. The BAM file should be generated with move tables and MD tags. You can refer to the tutorial below for guidance on how to generate such a BAM file.
                """
            ),
            
            pn.pane.Markdown(
                """
                ```sh
                dorado basecaller sup pod5/ --emit-moves | \\
                samtools fastq -T pt,mv,ts - | \\
                minimap2 -ax map-ont --MD -y ref.mmi - | \\
                samtools view -hb -F256 | samtools sort - > test.aln.bam
                ```
                """,
                renderer='markdown',
                sizing_mode='stretch_width'
            ),

            self.aln_input,

            self.load_dataset_button,
            self.load_example_button

        ),

        self.template = pn.template.BootstrapTemplate(title='Siren', sidebar=self.sidebar)
        # self.template = pn.template.MaterialTemplate(title='Siren', sidebar=self.sidebar)
        self.tabs = pn.Tabs(closable=True, sizing_mode='stretch_width')
        self.tabs.append(('Welcome', self.page_welcome()))
        self.toolbar = self.create_toolbar()
        self.layout = self.create_layout()
        self.template.main.append(self.layout)

    def page_welcome(self):
        welcome_text = """
        ## Welcome to Siren
        Siren Studio: A Modular and Extensible Interface for Nanopore Signal Data Analysis.
        """
        return pn.pane.Markdown(welcome_text, sizing_mode='stretch_both')

    def set_input(self, event):
        pass
        setup = FileSet()
        setup_content = setup.layout
        self.tabs.append(("Files", setup_content))

    def get_metadata(self, event):
        info = base.Info(self.file_signal, self.file_signal_bulk, self.file_aln)
        info_content = info.layout
        self.tabs.append(("File Info", info_content))

    def generate_plot(self, event):
        plot = base.Sigvis(self.file_signal)
        plot_content = plot.layout
        self.tabs.append(("Sigvis", plot_content))
    
    def generate_plot_bulk(self, event):
        plot = base.Bulkvis(self.file_signal_bulk)
        plot_content = plot.layout
        self.tabs.append(("Bulkvis", plot_content))

    def generate_plot_anchored(self, event):
        plot = base.Achovis(self.file_signal)
        plot_content = plot.layout
        self.tabs.append(("Achovis Read", plot_content))
    
    def generate_plot_anchored_ref(self, event):
        plot = base.AchovisRef(self.file_signal, self.file_aln)
        plot_content = plot.layout
        self.tabs.append(("Achovis Ref", plot_content))

    def generate_segmentation(self, event):
        plot = base.Segment(self.file_signal)
        plot_content = plot.layout
        self.tabs.append(("Segment", plot_content))

    def run_search(self, event):
        plot = base.Search(self.file_signal)
        plot_content = plot.layout
        self.tabs.append(("Search", plot_content))

    def create_toolbar(self):
        # Create toolbar with buttons and icons
        btn_file        = pn.widgets.Button(name='', button_type='primary', icon='file', icon_size='2em')
        btn_metadata    = pn.widgets.Button(name='', button_type='primary', icon='info-circle', icon_size='2em')
        btn_plot        = pn.widgets.Button(name='', button_type='primary', icon='wave-sine', icon_size='2em')
        btn_plot_bulk   = pn.widgets.Button(name='', button_type='primary', icon='tag', icon_size='2em')
        btn_tail        = pn.widgets.Button(name='', button_type='primary', icon='anchor', icon_size='2em')
        btn_anchor_read = pn.widgets.Button(name='', button_type='primary', icon='minus', icon_size='2em')
        btn_anchor_ref  = pn.widgets.Button(name='', button_type='primary', icon='align-justified', icon_size='2em')
        btn_segment     = pn.widgets.Button(name='', button_type='primary', icon='line-dashed', icon_size='2em')
        btn_search      = pn.widgets.Button(name='', button_type='primary', icon='database-search', icon_size='2em')
        btn_basecall    = pn.widgets.Button(name='', button_type='primary', icon='letter-case-upper')
        btn_motif       = pn.widgets.Button(name='', button_type='primary', icon='fingerprint')
        btn_split       = pn.widgets.Button(name='', button_type='primary', icon='line-dashed')
        btn_export      = pn.widgets.Button(name='', button_type='primary', icon='download')
        
        # Set up event handlers
        # btn_file.on_click(self.set_input)
        btn_metadata.on_click(self.get_metadata)
        btn_plot.on_click(self.generate_plot)
        btn_plot_bulk.on_click(self.generate_plot_bulk)
        # btn_anchor_read.on_click(self.generate_plot_anchored)
        btn_anchor_ref.on_click(self.generate_plot_anchored_ref)
        btn_segment.on_click(self.generate_segmentation)
        btn_search.on_click(self.run_search)
        # btn_basecall.on_click(self.basecall)
        # btn_motif.on_click(self.find_motif)
        # btn_split.on_click(self.find_split)
        # btn_export.on_click(self.export_pod5)
        
        toolbar = pn.Column(
            pn.Spacer(height=50),
            # btn_file,
            btn_metadata,
            btn_plot,
            btn_plot_bulk, 
            # btn_basecall,
            # btn_anchor_read,
            btn_anchor_ref,
            btn_segment,
            btn_search,
            # btn_motif,
            # btn_split,
            # btn_export
        )
        return toolbar
    
    def validate_file(self, format):
        return True
    
    def load_data(self, event):
        try:
            if self.signal_input.value:
                self.file_signal = self.signal_input.value
                format = self.file_signal.split('.')[-1]
                if format in ['pod5', 'fast5']:
                    self.validate_file(format=format)
                    pn.state.notifications.success('Signal data loaded successfully.', duration=2000)
                else:
                    raise ValueError("Invalid format. Please check if the file is 'pod5' or 'fast5'.")
            
            if self.bulk_input.value:
                self.file_signal_bulk = self.bulk_input.value
                format = self.file_signal_bulk.split('.')[-1]
                if format in ['fast5']:
                    self.validate_file(format=format)
                    pn.state.notifications.success('Bulk data loaded successfully.', duration=2000)
                else:
                    raise ValueError("Invalid format. Please check if the file is 'fast5'.")
            
            if self.aln_input.value:
                self.file_aln = self.aln_input.value
                format = self.file_aln.split('.')[-1]
                if format in ['bam', 'sam']:
                    self.validate_file(format=format)
                    pn.state.notifications.success('Alignment signal loaded successfully.', duration=2000)
                else:
                    raise ValueError("Invalid format. Please check if the file is 'bam' or 'sam'.")
            
            if (self.file_signal_bulk == None) and (self.file_signal == None):
                raise ValueError("Signal data not provided. Please provide either signal or bulk signal data.")
        
        except Exception as e:
            # Handle any errors during the loading process
            pn.state.notifications.error(f"Data load failed: {e}", duration=3000)

    
    def create_layout(self):
        layout = pn.Row(
            pn.Column(self.toolbar, width=80),
            pn.Column(self.tabs),
            # pn.Row(
            #     # pn.Spacer(height=20),
            #     self.tabs
            # ),
            # pn.Row(
            #     # pn.Spacer(width=30),
            #     pn.Column(self.toolbar, width=50),
            #     # pn.Column(self.tabs),  # Tab section
            # )
        )

        return layout
    
    def servable(self):
        self.template.servable()

# Custom CSS
raw_css = """
.bk-header { width: 100px; }
.mdc-dialog__content { background-color: white; }
"""

pn.extension(raw_css=[raw_css])

# Instantiate and serve the app
app = Siren()
app.servable()
