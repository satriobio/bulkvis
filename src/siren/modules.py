# Standard Library
import json
import random
from itertools import chain

# Third-Party Libraries
import h5py
import numpy as np
import pandas as pd
import pysam
import ruptures as rpt

# Bokeh Libraries
from bokeh.models import ColumnDataSource, Label, RangeSlider, Text
from bokeh.palettes import Category10
from bokeh.plotting import figure, show
from bokeh.resources import INLINE

# Panel Libraries
import panel as pn
import panel.widgets as widgets

# External Libraries
from pileupy.main import Pileupy
from remora import io, refine_signal_map
from siren.utils.squiggletools import BulkFile, SquiggleFile

# Pod5
import pod5

class Info:
    def __init__(self, file_signal=None, file_signal_bulk=None, file_aln=None):
        self.layout = None

        self.file_signal = file_signal
        self.file_signal_bulk = file_signal_bulk
        self.file_aln = file_aln

        try:
            self._load_data()
            self._setup_layout()
            pn.state.notifications.success('Plugin loaded successfully.', duration=2000)
        except Exception as e:
            self.layout = pn.Row(
                pn.Column(
                    """
                    ## Dataset Info
                    Please check input
                    """
                ),
                margin=(20, 20, 20, 20)  
            )
            print(f"An error occurred: {e}")
            pn.state.notifications.warning(f"Plugin load failed. Error:{e}", duration=2000)
    
    def _load_data(self):
        if self.file_signal or self.file_signal_bulk or self.file_aln:
            metadata_signal = None
            metadata_bulk = None
            metadata_aln = None
            
            if self.file_signal:
                self.reader_signal = SquiggleFile(self.file_signal)
                metadata_signal = {
                    'read count': len(self.reader_signal.list_reads())
                }

            if self.file_signal_bulk:
                metadata_bulk = {}
                self.reader_signal = BulkFile(self.file_signal_bulk)
                metadata_bulk = {
                    'channel count': len(self.reader_signal.list_channels())
                }

            if self.file_aln:
                metadata_aln = {}
                self.reader_aln = pysam.AlignmentFile(self.file_aln)

                read = next(self.reader_aln.fetch(), None)
                
                if read.has_tag('MD'):
                    md = True
                else:
                    md = False

                if read.has_tag('mv'):
                    mv = True
                else:
                    mv = False

                metadata_aln = {
                    'MD tags': md,
                    'Move tables': mv,
                }
            
            input_json = json.dumps({
                "Signal Data": metadata_signal,
                "Bulk Signal Data": metadata_bulk,
                "Alignment Data": metadata_aln
            }, indent=2)
            
            self.json_view = pn.pane.JSON(json.loads(input_json), name='Metadata', theme='light', depth=-1)
        else:
            raise Exception("") 

    def _setup_layout(self):
        self.layout = pn.Row(
            pn.Column(
                "## Dataset Info",
                self.json_view
            ),
            margin=(20, 20, 20, 20)  
        )

class Base:
    def __init__(self, file_signal, start_time=0, end_time=0):
        # Data
        self.file_signal = file_signal
        self.df_signal = []
        self.df_segmentation = []

        # Components
        self.layout = None
        self.figure = None
        self.p1 = None
        self.p2 = None
        self.rslider = None
        self.start_time = start_time
        self.end_time = end_time

        # Widgets
        self.input_read_id = widgets.TextInput(name="Read ID")
        self.input_start_time = widgets.IntInput(name='Start Time', start=0, step=1)
        self.input_end_time = widgets.IntInput(name='End Time', start=0, step=1)
        self.input_norm = widgets.Select(name='Normalization', options=['None', 'Z-Score', 'MAD'])
        self.input_seg = widgets.Select(name='Segmentation', options=['None', 'Ruptures'])
        self.button_plot = widgets.Button(name='Plot', button_type='primary', sizing_mode='stretch_width')
        self.button_export = widgets.Button(name='Export Report', button_type='success', sizing_mode='stretch_width')
        
        # Events
        self.button_plot.on_click(self._update_plot)
        self.button_export.on_click(self._export_layout)

    def norm_z_score(self, signal):
        """
        Normalize the input signal using z-score normalization.

        Args:
            signal (np.ndarray): Input 1D signal array.

        Returns:
            np.ndarray: Z-score normalized signal.
        """
        mean = np.mean(signal)
        std = np.std(signal)
        return (signal - mean) / std

    def norm_mad(self, signal):
        """
        Normalize the input signal using MAD normalization.

        Args:
            signal (np.ndarray): Input 1D signal array.

        Returns:
            np.ndarray: MAD normalized signal.
        """
        median = np.median(signal)
        mad = np.median(np.abs(signal - median))
        return (signal - median) / mad if mad != 0 else signal - median

    def segment_rpt(self, signal):
        """
        Segment the input signal using the Ruptures package (bottom-up segmentation).

        Args:
            signal (np.ndarray): Input 1D signal array.

        Returns:
            list: List of breakpoints indicating the segmentation points.
        """
        n_bkps = 3  # Number of breakpoints to detect
        algo = rpt.BottomUp(model="l2")
        algo.fit(signal)
        breakpoints = algo.predict(n_bkps=n_bkps)
        return breakpoints

    def _load_data(self, time_range='0-0'):
        self.df_signal = []
        self.df_segmentation = []
        
        read_ids = [self.input_read_id.value]  # Adjusted to use the widget's value
        norm = self.input_norm.value  # Adjusted to use the widget's value
        segment = self.input_seg.value  # Adjusted to use the widget's value

        if read_ids:
            for read_id in read_ids:
                signal = self.reader_signal.fetch_squiggle(f'{read_id}')

                if norm == 'Z-Score':
                    signal = self.norm_z_score(signal)
                if norm == 'MAD':
                    signal = self.norm_mad(signal)
                if segment == 'Ruptures':
                    self.df_segmentation = self.segment_rpt(signal)

                self.df_signal.append(pd.DataFrame({'Time': range(len(signal)), 'Value': signal}))

    def _generate_plot(self):
        start = self.start_time
        end = self.end_time

        self.p1 = figure(min_border_left=50, min_border_right=50, toolbar_location=None, 
                         x_range=(start, end), sizing_mode='stretch_width')
        self.p1.toolbar.logo = None
        self.p1.toolbar.active_drag = None
        self.p1.toolbar.active_scroll = None
        self.p1.toolbar.active_tap = None

        colors = Category10[10]
        filtered_data_list = []
        
        if self.df_signal:
            for df_signal in self.df_signal:
                filtered_data = df_signal[(df_signal['Time'] >= start) & (df_signal['Time'] <= end)]
                filtered_data_list.append(filtered_data)
            
            for idx, filtered_data in enumerate(filtered_data_list):
                self.p1.line('Time', 'Value', source=filtered_data, color=colors[idx % len(colors)])

        if self.df_segmentation:
            for breakpoint in self.df_segmentation:
                self.p1.vspan(x=breakpoint, line_dash = 'dashed', line_color='red', line_width=2)
                # self.p1.vspan(x=row['acquisition_raw_index'], line_dash = 'dashed', line_color='red', line_width=2)

        self.p1.ygrid.grid_line_color = None
        self.p1.background_fill_color = "#fafafa"

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

        if filtered_data_list:
            for idx, filtered_data in enumerate(filtered_data_list):
                self.p2.line('Time', 'Value', source=filtered_data, color=colors[idx % len(colors)])

        self.rslider = RangeSlider(margin=50, start=start, end=end, value=(start, end), title=None, show_value=False, 
                                   sizing_mode='stretch_width')
        self.rslider.js_link('value', self.p1.x_range, 'start', attr_selector=0)
        self.rslider.js_link('value', self.p1.x_range, 'end', attr_selector=1)

        self.figure = pn.Column(self.p1, self.p2, self.rslider, sizing_mode='stretch_width')

    def _update_plot(self, event=None):
        self.start_time = self.input_start_time.value  # Updated to use widget values
        self.end_time = self.input_end_time.value  # Updated to use widget values
        self._load_data(f'{self.start_time}-{self.end_time}')
        self._generate_plot()
        self.layout[1] = pn.Column(self.figure)

    def _setup_layout(self):
        self.layout = pn.Row(
            pn.Column(
                pn.WidgetBox(
                    '''
                    # Signal Visualization
                    ''',
                    self.input_read_id,
                    self.input_start_time,
                    self.input_end_time,
                    self.input_norm,
                    self.input_seg,
                    self.button_plot,
                    self.button_export,
                    width=330
                ),
            ),
            self.figure,
            margin=(20, 20, 20, 20)  
        )

    def _export_layout(self, event):
        self.layout.save('sigvis.html', resources=INLINE)
        pn.state.notifications.success('Report exported successfully.', duration=2000)

class Sigvis(Base):
    def __init__(self, file_signal):
        super().__init__(file_signal)

        try:
            self.reader_signal = SquiggleFile(self.file_signal)

            self._generate_plot()
            self._setup_layout()
            pn.state.notifications.success('Plugin loaded successfully.', duration=2000)
        except Exception as e:
            self.layout = pn.Row(
                pn.Column(
                    """
                    ## Sigvis
                    Error loading input.
                    """
                ),
                margin=(20, 20, 20, 20)  
            )
            print(f"An error occurred: {e}")
            pn.state.notifications.warning(f"Plugin load failed. Error:{e}", duration=2000)

class Bulkvis(Base):
    def __init__(self, file_signal):
        super().__init__(file_signal)

        # Data
        self.df_annotation = None

        # Widgets
        self.input_channel_id = widgets.TextInput(name="Channel ID")
        self.input_annotation = pn.widgets.MultiChoice(name='Annotation', value=[], options=[])

        try:
            self.reader_signal = BulkFile(self.file_signal)
            self._generate_plot()
            self._setup_layout()
            pn.state.notifications.success('Plugin loaded successfully.', duration=2000)
        except Exception as e:
            self.layout = pn.Row(
                pn.Column(
                    """
                    ## Bulkvis
                    Error loading input.
                    """
                ),
                margin=(20, 20, 20, 20)  
            )
            print(f"An error occurred: {e}")
            pn.state.notifications.warning(f"Plugin load failed. Error:{e}", duration=2000)
    
    def _load_data(self, time_range='0-0'):
        self.df_signal = []
        self.df_segmentation = []
        
        channel_ids = [self.input_channel_id.value]

        if channel_ids:
            for channel_id in channel_ids:
                signal = self.reader_signal.fetch_squiggle(f'{channel_id}:{self.start_time}-{self.end_time}')
                self.df_signal.append(pd.DataFrame({'Time': range(self.start_time, self.end_time), 'Value': signal}))
                annotation = self.reader_signal.fetch_annotation(f'{channel_id}:{time_range}')
                annotation_types = h5py.check_dtype(enum=annotation.dtype['summary_state'])
                annotation_types = {v: k for k, v in annotation_types.items()}
                self.df_annotation = pd.DataFrame(annotation)
                self.df_annotation['summary_state'] = self.df_annotation['summary_state'].map(annotation_types)
                self.input_annotation.options = list(annotation_types.values())

    def _generate_plot(self):
            start = self.start_time
            end = self.end_time

            self.p1 = figure(min_border_left=50, min_border_right=50, toolbar_location=None, sizing_mode='stretch_width')
            self.p1.toolbar.logo = None
            self.p1.toolbar.active_drag = None
            self.p1.toolbar.active_scroll = None
            self.p1.toolbar.active_tap = None

            colors = Category10[10]
            filtered_data_list = []
            
            if self.df_signal:
                for df_signal in self.df_signal:
                    filtered_data = df_signal[(df_signal['Time'] >= start) & (df_signal['Time'] <= end)]
                    filtered_data_list.append(filtered_data)
                
                for idx, filtered_data in enumerate(filtered_data_list):
                    print('here')
                    print(filtered_data)
                    self.p1.line('Time', 'Value', source=filtered_data, color=colors[idx % len(colors)])

            if self.df_segmentation:
                for breakpoint in self.df_segmentation:
                    self.p1.vspan(x=breakpoint, line_color='gray', line_width=2)

            self.p1.ygrid.grid_line_color = None
            self.p1.background_fill_color = "#fafafa"

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

            if filtered_data_list:
                for idx, filtered_data in enumerate(filtered_data_list):
                    self.p2.line('Time', 'Value', source=filtered_data, color=colors[idx % len(colors)])

            self.rslider = RangeSlider(margin=50, start=start, end=end, value=(start, end), title=None, show_value=False, 
                                    sizing_mode='stretch_width')
            self.rslider.js_link('value', self.p1.x_range, 'start', attr_selector=0)
            self.rslider.js_link('value', self.p1.x_range, 'end', attr_selector=1)

            self.figure = pn.Column(self.p1, self.p2, self.rslider, sizing_mode='stretch_width')

            if isinstance(self.df_annotation, pd.DataFrame):
                if len(self.df_annotation) > 0:
                    offset = self.df_signal[0]['Value'].max() - (0.3 * (self.df_signal[0]['Value'].max() - self.df_signal[0]['Value'].min()))
                    df_annotation_active = self.df_annotation[self.df_annotation['summary_state'].isin(self.input_annotation.value)]

                    # Generate a random color for each unique value in 'summary_state'
                    unique_states = df_annotation_active['summary_state'].unique()
                    colors = {state: f'#{random.randint(0, 0xFFFFFF):06x}' for state in unique_states}
                    for _, row in df_annotation_active.iterrows():
                        self.p1.vspan(x=row['acquisition_raw_index'], line_dash = 'dashed', line_color=colors[row['summary_state']], line_width=2)
                        
                        # Create a label instead of text
                        label = Label(x=row['acquisition_raw_index'], y=offset, text=row['summary_state'], 
                                    text_color='white', text_align='left', text_baseline='middle', angle_units="deg", angle=45, background_fill_color=colors[row['summary_state']])
                        self.p1.add_layout(label)

                    self.figure = pn.Column(self.p1, self.p2, self.rslider, sizing_mode='stretch_width')

    def _setup_layout(self):
        self.layout = pn.Row(
            pn.Column(
                pn.WidgetBox(
                    '''
                    # Bulk Signal Visualization
                    ''',
                    self.input_channel_id,
                    self.input_start_time,
                    self.input_end_time,
                    self.input_annotation,
                    self.button_plot,
                    self.button_export,
                    width=330
                ),
            ),
            self.figure,
            margin=(20, 20, 20, 20)  
        )

class AchovisRef(Base):
    def __init__(self, file_signal, file_aln):
        super().__init__(file_signal)

        self.file_aln = file_aln

        # Data
        self.BASE_COLORS = {"A": "#00CC00", "C": "#0000CC", "G": "#FFB300", "T": "#CC0000", "U": "#CC0000", "N": "#FFFFFF"}
        self.file_kmer_levels = file_kmer_levels = "/mnt/869990e7-a61f-469f-99fe-a48d24ac44ca/git/phd-miten/9mer_levels_v1.txt"

        # Widgets
        self.input_reference_file = widgets.TextInput(name="Reference Path")
        self.input_reference = widgets.TextInput(name="Contig Name")
        self.input_levels = pn.widgets.Checkbox(name='Levels')
        self.input_basecall_seq = pn.widgets.Checkbox(name='Basecall Sequence')
        # self.input_stack = pn.widgets.Checkbox(name='Stack signal')

        try:
            self.reader_signal = pod5.DatasetReader(file_signal)
            self.reader_aln = io.ReadIndexedBam(file_aln) 
            self._generate_plot()
            self._setup_layout()
            pn.state.notifications.success('Plugin loaded successfully.', duration=2000)
        except Exception as e:
            self.layout = pn.Row(
                pn.Column(
                    """
                    ## Achovis
                    Error loading input.
                    """
                ),
                margin=(20, 20, 20, 20)  
            )
            print(f"An error occurred: {e}")
            pn.state.notifications.warning(f"Plugin load failed. Error:{e}", duration=2000)

    def _load_data(self, event):
        pass

    def _generate_plot(self):
        ref = self.input_reference_file.value
        contig = self.input_reference.value
        start = self.start_time
        end = self.end_time

        self.p1 = figure(x_axis_label="Reference Position", y_axis_label="Normalized Signal", sizing_mode='stretch_width')

        # self.p1.toolbar_location=None
        # self.p1.toolbar.logo = None
        # self.p1.toolbar.active_drag = None
        # self.p1.toolbar.active_scroll = None
        # self.p1.toolbar.active_tap = None

        self.p1.ygrid.grid_line_color = None
        self.p1.background_fill_color = "#fafafa"

        self.p2 = figure(min_border_left=50, min_border_right=50, height=80, sizing_mode='stretch_width')
        # self.p2.toolbar.logo = None
        # self.p2.toolbar.active_drag = None
        # self.p2.toolbar.active_scroll = None
        # self.p2.toolbar.active_tap = None

        # self.p2.toolbar_location=None
        self.p2.yaxis.major_label_text_color = None
        self.p2.yaxis.major_tick_line_color = None
        self.p2.yaxis.minor_tick_line_color = None
        self.p2.grid.grid_line_color = None

        if contig:
            sig_map_refiner = refine_signal_map.SigMapRefiner(
                kmer_model_filename=self.file_kmer_levels, do_rough_rescale=True, scale_iters=0, do_fix_guage=True
            )

            ref_reg = io.RefRegion(ctg=contig, strand="+", start=start, end=end)
            ref_seq = io.get_ref_seq_from_reads(ref_reg, io.get_reg_bam_reads(ref_reg, self.reader_aln))

            samples_read_ref_regs, reg_bam_reads = io.get_reads_reference_regions(ref_reg, [(self.reader_signal, self.reader_aln)], sig_map_refiner=sig_map_refiner)
            seq, levels = io.get_ref_seq_and_levels_from_reads(ref_reg, chain(*reg_bam_reads), sig_map_refiner)

            df_signal = pd.concat([
                pd.DataFrame({
                    "Reference Position": np.concatenate([read.ref_sig_coords for read in s_reads]),
                    "Signal": np.concatenate([read.norm_signal for read in s_reads]),
                    "Read": [f"{sample_name}_{read_idx}" for read_idx, read in enumerate(s_reads) for _ in range(read.norm_signal.size)],
                }) for sample_name, s_reads in zip(["Control"], samples_read_ref_regs)
            ])

            df_test = pd.concat([
                pd.DataFrame({
                    "chrom": contig,
                    "start": np.concatenate([read.ref_sig_coords for read in s_reads]),
                    "end": np.concatenate([read.ref_sig_coords for read in s_reads]) + 1,
                    "value": np.concatenate([read.norm_signal for read in s_reads]),
                    "read_names": [f"{sample_name}_{read_idx}" for read_idx, read in enumerate(s_reads) for _ in range(read.norm_signal.size)],
                }) for sample_name, s_reads in zip(["Control"], samples_read_ref_regs)
            ])

            df_level = pd.DataFrame({
                "base_st": np.arange(ref_reg.start, ref_reg.end),
                "base_en": np.arange(ref_reg.start + 1, ref_reg.end + 1),
                "level": levels
            })

            offset = df_signal['Signal'].min() - (0.1 *(df_signal['Signal'].max() - df_signal['Signal'].min()))
            df_base = pd.DataFrame({
                "x": np.arange(ref_reg.start, ref_reg.end),
                "y": offset,  # Position the text at the bottom
                "base": [base for base in seq],
                "text_color": [self.BASE_COLORS[b] for b in seq]
            })

            for sample_name, sample_df in df_signal.groupby('Read'):
                self.p1.line(x='Reference Position', y='Signal', source=sample_df, line_width=1, alpha=0.1, color="red")

            if self.input_basecall_seq.value:
                self.p1.add_glyph(ColumnDataSource(df_base), Text(x="x", y="y", text="base", text_color="text_color", text_font_size="12pt"))

            if self.input_levels.value:
                self.p1.segment(x0='base_st', x1='base_en', y0='level', y1='level', source=df_level, line_width=2, color='orange', legend_label="Levels")

            # if filtered_data_list:
            for sample_name, sample_df in df_signal.groupby('Read'):
                self.p2.line('Time', 'Value', source=sample_df, color="red")

            self.rslider = RangeSlider(margin=50, start=start, end=end, value=(start, end), title=None, show_value=False, 
                                    sizing_mode='stretch_width')
            self.rslider.js_link('value', self.p1.x_range, 'start', attr_selector=0)
            self.rslider.js_link('value', self.p1.x_range, 'end', attr_selector=1)

        # self.figure = pn.Column(self.p1, self.p2, self.rslider, sizing_mode='stretch_width')

            browser = Pileupy(f'{contig}:{start}-{end}', reference=ref)
            browser.add_track_alignment(self.file_aln, height=200)
            browser.add_track_df(df_test, height=200)
            self.figure = pn.Column(*[track.figure for track in browser.tracks])
        # self.figure = pn.Column(self.p1)

    def _setup_layout(self):
        self.layout = pn.Row(
            pn.Column(
                pn.WidgetBox(
                    '''
                    # Anchored to Reference Visualization
                    ''',
                    self.input_reference_file,
                    self.input_reference,
                    self.input_start_time,
                    self.input_end_time,
                    # self.input_levels,
                    # self.input_basecall_seq,
                    self.button_plot,
                    self.button_export,
                    width=330
                ),
            ),
            self.figure,
            margin=(20, 20, 20, 20)  
        )        

class Segment():
    def __init__(self, file_signal):
        # Data
        self.file_signal = file_signal
        self.df_signal = []
        self.df_segmentation = []

        # Components
        self.layout = None
        self.figure = None
        
        # Widgets
        self.input_seg = widgets.Select(name='Segmentation', options=['Teri', 'Nanopolish'])

        # Events
        self.button_plot = widgets.Button(name='Plot', button_type='primary', sizing_mode='stretch_width')
        self.button_export = widgets.Button(name='Export Data', button_type='success', sizing_mode='stretch_width')

        try:
            self._generate_plot()
            self._setup_layout()
            pn.state.notifications.success('Plugin loaded successfully.', duration=2000)
        except Exception as e:
            self.layout = pn.Row(
                pn.Column(
                    """
                    ## Sigvis
                    Invalid input.
                    """
                ),
                margin=(20, 20, 20, 20)  
            )
            print(f"An error occurred: {e}")
            pn.state.notifications.warning(f"Plugin load failed. Error:{e}", duration=2000)

    def _load_data(self, time_range='0-0'):
        self.df_signal = []
        self.df_segmentation = []
        
        read_ids = [self.input_read_id.value]  # Adjusted to use the widget's value
        norm = self.input_norm.value  # Adjusted to use the widget's value
        segment = self.input_seg.value  # Adjusted to use the widget's value

        if read_ids:
            for read_id in read_ids:
                signal = self.reader_signal.fetch_squiggle(f'{read_id}')

                if norm == 'Z-Score':
                    signal = self.norm_z_score(signal)
                if norm == 'MAD':
                    signal = self.norm_mad(signal)
                if segment == 'Ruptures':
                    self.df_segmentation = self.segment_rpt(signal)

                self.df_signal = signal

                # calculate poly a len
                # find breakpoints

    def _generate_plot(self):

        self.p1 = figure(min_border_left=50, min_border_right=50, toolbar_location=None, 
                    x_range=(0, 10), sizing_mode='stretch_width')
        self.p1.toolbar.logo = None
        self.p1.toolbar.active_drag = None
        self.p1.toolbar.active_scroll = None
        self.p1.toolbar.active_tap = None

        df = pd.DataFrame({
            'int': [1, 2, 3],
            'float': [3.14, 6.28, 9.42],
            'str': ['A', 'B', 'C'],
            'bool': [True, False, True],
        }, index=[1, 2, 3])

        df_pane = pn.Column("## Segmentation and Tail Length", pn.pane.DataFrame(df, max_rows=1, sizing_mode='stretch_width'), margin=(0, 40, 0, 40))

        self.figure = pn.Column(df_pane, self.p1, sizing_mode='stretch_width')

    def _update_plot(self, event=None):
        self.start_time = self.input_start_time.value  # Updated to use widget values
        self.end_time = self.input_end_time.value  # Updated to use widget values
        self._load_data(f'{self.start_time}-{self.end_time}')
        self._generate_plot()
        self.layout[1] = pn.Column(self.figure)

    def _setup_layout(self):
        self.layout = pn.Row(
            pn.Column(
                pn.WidgetBox(
                    '''
                    # Signal Segmentation
                    ''',
                    self.input_seg,
                    self.button_plot,
                    self.button_export,
                    width=330
                ),
            ),
            self.figure,
            margin=(20, 20, 20, 20)  
        )

    def _export_layout(self, event):
        self.layout.save('sigvis.html', resources=INLINE)
        pn.state.notifications.success('Report exported successfully.', duration=2000)

class Search(Segment):
    def __init__(self, file_signal):
        # Data
        self.file_signal = file_signal
        self.df_signal = []
        self.df_segmentation = []

        # Components
        self.layout = None
        self.figure = None
        
        # Widgets
        self.input_seq = widgets.TextInput(name='Sequence')

        # Events
        self.button_plot = widgets.Button(name='Plot', button_type='primary', sizing_mode='stretch_width')
        self.button_export = widgets.Button(name='Export Report', button_type='success', sizing_mode='stretch_width')

        try:
            self._generate_plot()
            self._setup_layout()
            pn.state.notifications.success('Plugin loaded successfully.', duration=2000)
        except Exception as e:
            print(f"An error occurred: {e}")
            pn.state.notifications.warning(f"Plugin load failed. Error:{e}", duration=2000)

    def _setup_layout(self):
        self.layout = pn.Row(
            pn.Column(
                pn.WidgetBox(
                    '''
                    # Search Sequence
                    ''',
                    self.input_seq,
                    self.button_plot,
                    self.button_export,
                    width=330
                ),
            ),
            self.figure,
            margin=(20, 20, 20, 20)  
        )

flexbox_css = """
.flex-end-row {
    display: flex;
    justify-content: flex-end; /* Align children to the right */
    width: 100%; /* Make sure the row spans full width */
}
"""

# Add custom CSS to the Panel extension
pn.config.raw_css.append(flexbox_css)
