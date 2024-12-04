import panel as pn
import panel.widgets as widgets

from bokeh.plotting import figure
from bokeh.models import RangeSlider
from bokeh.palettes import Category10 
import plotly.graph_objects as go

import h5py
import pandas as pd

from siren.utils.squiggletools import BulkFile, SquiggleFile
from bokeh.resources import INLINE
import ruptures as rpt

from remora import io, refine_signal_map
from itertools import chain

import numpy as np
from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource, Text

import pod5

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
        self.input_seg = widgets.Select(name='Segmentation', options=['None', 'Ruptures', 'Model'])
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
        
        # Reader
        self.reader_signal = SquiggleFile(self.file_signal)

        try:
            self._generate_plot()
            self._setup_layout()
            pn.state.notifications.success('Plugin loaded successfully.', duration=2000)
        except Exception as e:
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

        # Reader
        self.reader_signal = BulkFile(self.file_signal)

        try:
            self._generate_plot()
            self._setup_layout()
            pn.state.notifications.success('Plugin loaded successfully.', duration=2000)
        except Exception as e:
            print(f"An error occurred: {e}")
            pn.state.notifications.warning(f"Plugin load failed. Error:{e}", duration=2000)
    
    def _load_data(self, time_range='0-0'):
        self.df_signal = []
        self.df_segmentation = []
        
        channel_ids = [self.input_channel_id.value]

        if channel_ids:
            for channel_id in channel_ids:
                signal = self.reader_signal.fetch_squiggle(f'{channel_id}:{time_range}')
                self.df_signal.append(pd.DataFrame({'Time': range(len(signal)), 'Value': signal}))

                annotation = self.reader_signal.fetch_annotation(f'{channel_id}:{time_range}')
                annotation_types = h5py.check_dtype(enum=annotation.dtype['summary_state'])
                annotation_types = {v: k for k, v in annotation_types.items()}
                self.df_annotation = pd.DataFrame(annotation)
                self.df_annotation['summary_state'] = self.df_annotation['summary_state'].map(annotation_types)
                self.input_annotation.options = list(annotation_types.values())

    def _generate_plot(self):
        super()._generate_plot()

        if isinstance(self.df_annotation, pd.DataFrame):
            if len(self.df_annotation) > 0:
                df_annotation_active = self.df_annotation[self.df_annotation['summary_state'].isin(self.input_annotation.value)]
                for _, row in df_annotation_active.iterrows():
                    self.p1.vspan(x=row['acquisition_raw_index'], line_color='gray', line_width=2)
                    
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

        # Data
        self.BASE_COLORS = {"A": "#00CC00", "C": "#0000CC", "G": "#FFB300", "T": "#CC0000", "U": "#CC0000", "N": "#FFFFFF"}
        self.file_kmer_levels = file_kmer_levels = "/mnt/869990e7-a61f-469f-99fe-a48d24ac44ca/git/phd-miten/9mer_levels_v1.txt"

        # Widgets
        self.input_reference = widgets.TextInput(name="Reference Name")
        self.input_levels = pn.widgets.Checkbox(name='Levels')
        self.input_basecall_seq = pn.widgets.Checkbox(name='Basecall Sequence')
        # self.input_stack = pn.widgets.Checkbox(name='Stack signal')


        # Reader
        self.reader_signal = pod5.DatasetReader(file_signal)
        self.reader_aln = io.ReadIndexedBam(file_aln) 

        try:
            self._generate_plot()
            self._setup_layout()
            pn.state.notifications.success('Plugin loaded successfully.', duration=2000)
        except Exception as e:
            print(f"An error occurred: {e}")
            pn.state.notifications.warning(f"Plugin load failed. Error:{e}", duration=2000)

    def _load_data(self, event):
        pass

    def _generate_plot(self):
        contig = self.input_reference.value
        start = self.start_time
        end = self.end_time

        self.p1 = figure(toolbar_location=None, x_axis_label="Reference Position", y_axis_label="Normalized Signal", sizing_mode='stretch_width')
        self.p1.toolbar.logo = None
        self.p1.toolbar.active_drag = None
        self.p1.toolbar.active_scroll = None
        self.p1.toolbar.active_tap = None
        self.p1.ygrid.grid_line_color = None
        self.p1.background_fill_color = "#fafafa"

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
                self.p1.line(x='Reference Position', y='Signal', source=sample_df, line_width=1.5, alpha=0.7, color="red")

            if self.input_basecall_seq.value:
                self.p1.add_glyph(ColumnDataSource(df_base), Text(x="x", y="y", text="base", text_color="text_color", text_font_size="12pt"))

            if self.input_levels.value:
                self.p1.segment(x0='base_st', x1='base_en', y0='level', y1='level', source=df_level, line_width=2, color='blue', legend_label="Levels")
        
        self.figure = pn.Column(self.p1)

    def _setup_layout(self):
        self.layout = pn.Row(
            pn.Column(
                pn.WidgetBox(
                    '''
                    # Anchored to Reference Visualization
                    ''',
                    self.input_reference,
                    self.input_start_time,
                    self.input_end_time,
                    self.input_levels,
                    self.input_basecall_seq,
                    self.button_plot,
                    self.button_export,
                    width=330
                ),
            ),
            self.figure,
            margin=(20, 20, 20, 20)  
        )        

class AchovisRef2(Base):
    def __init__(self, signal_fn, aln_fn):

        # self.file_type = "fast5"
        # self.group_ids = ["Channel_50"]
        # self.file_signal = BulkFile(file_fn)
        self.signal_fn = pod5.DatasetReader(signal_fn)
        self.aln_fn = io.ReadIndexedBam(aln_fn) 

        self.df_annotation = None
        self.annotation_widget = pn.widgets.MultiChoice(name='Annotation', value=[], options=[])
        # super().__init__(file_fn)

        self.reference_widget = widgets.TextInput(name="Reference Name")
        self.df_squiggle = None
        self.segmentation = None
        self.p1 = None
        self.p2 = None
        self.rslider = None
        self.figure = None
        self.layout = None
        self.start_time = 0
        self.end_time = 200

        self.start = widgets.IntInput(name="Alignment Start")
        self.end = widgets.IntInput(name="Alignment Start")
        self.level = pn.widgets.Checkbox(name='Display levels')
        self.basecall = pn.widgets.Checkbox(name='Display basecall')

        self.group_ids_widget = widgets.TextInput(name="Channel ID")
        self.start_time_widget = widgets.IntInput(name='Start Time', start=0, step=1)
        self.end_time_widget = widgets.IntInput(name='End Time', start=0, step=1)
        self.plot_button = widgets.Button(name='Plot', button_type='primary', sizing_mode='stretch_width')
        self.plot_button.on_click(self._update_plot)

        self.export_button = widgets.Button(name='Export Report', button_type='success', sizing_mode='stretch_width')
        self.export_button.on_click(self._export_layout)        

        self.refresh_button = widgets.Button(name='Refresh')
        self.refresh_button.on_click(self._update_plot)
        self.tabs = pn.Tabs(tabs_location='above', sizing_mode='stretch_both')

        try:
        #     self._load_data()
            self._generate_plot()
            self._setup_layout()
            pn.state.notifications.success('Plugin loaded successfully.', duration=2000)
        except Exception as e:
            print(f"An error occurred: {e}")
    
    def _load_data(self, time_range='0-0'):
        pass
        # super()._load_data(time_range)
        # self.df_squiggle = []
        # self.group_ids = [self.group_ids_widget.value]
        # squiggle = self.file_signal.fetch_squiggle(f'{self.group_ids[0]}:{time_range}')
        # df_squiggle = pd.DataFrame({'Time': range(len(squiggle)), 'Value': squiggle})
        # self.df_squiggle.append(df_squiggle)
        
        # annotation = self.file_signal.fetch_annotation(f'{self.group_ids[0]}:{time_range}')
        # annotation_types = h5py.check_dtype(enum=annotation.dtype['summary_state'])
        # annotation_types = {v: k for k, v in annotation_types.items()}
        # self.df_annotation = pd.DataFrame(annotation)
        # self.df_annotation['summary_state'] = self.df_annotation['summary_state'].map(annotation_types)
        # self.annotation_widget.options = list(annotation_types.values())

    def _validate_input(self, input):
            reference_widget = input.split(':')
            contig, start, end = (reference_widget[0], reference_widget[1].split('-')[0], reference_widget[1].split('-')[1])

            if (len(contig) > 0) and start.isdigit() and end.isdigit():
                return True
            else:
                return False

    def _generate_plot(self):
        
        # pod5_dr = pod5.DatasetReader("/mnt/869990e7-a61f-469f-99fe-a48d24ac44ca/git/phd-miten/pod5/FAY61482_543ee3d4_5580dca4_0.pod5")
        # bam_fh = io.ReadIndexedBam("/mnt/869990e7-a61f-469f-99fe-a48d24ac44ca/git/phd-miten/test.aln.bam")

        # "ENST00000610460.1|ENSG00000277739.1|-|-|5_8S_rRNA.5-201|5_8S_rRNA|153|rRNA|:20-50"
        
        p = figure(toolbar_location=None, x_axis_label="Reference Position", y_axis_label="Normalized Signal", sizing_mode='stretch_width')

        
        try:
            # contig, region = self.reference_widget.value.split(":")
            # start, end = map(int, region.split("-"))

            start = self.start.value
            end = self.end.value
            contig = self.reference_widget.value

            if contig:


                sig_map_refiner = refine_signal_map.SigMapRefiner(
                    kmer_model_filename=level_table, do_rough_rescale=True, scale_iters=0, do_fix_guage=True
                )

                ref_reg = io.RefRegion(ctg=contig, strand="+", start=start, end=end)
                ref_seq = io.get_ref_seq_from_reads(ref_reg, io.get_reg_bam_reads(ref_reg, self.aln_fn))


                print('here')
                # Get the reference region reads
                samples_read_ref_regs, reg_bam_reads = io.get_reads_reference_regions(ref_reg, [(self.signal_fn, self.aln_fn)], sig_map_refiner=sig_map_refiner)

                # Get the sequence and levels
                seq, levels = io.get_ref_seq_and_levels_from_reads(ref_reg, chain(*reg_bam_reads), sig_map_refiner)

                # Prepare the signal dataframe
                sig_df = pd.concat([
                    pd.DataFrame({
                        "Reference Position": np.concatenate([read.ref_sig_coords for read in s_reads]),
                        "Signal": np.concatenate([read.norm_signal for read in s_reads]),
                        "Read": [f"{sample_name}_{read_idx}" for read_idx, read in enumerate(s_reads) for _ in range(read.norm_signal.size)],
                    }) for sample_name, s_reads in zip(["Control"], samples_read_ref_regs)
                ])

                # Prepare the levels dataframe
                df_level = pd.DataFrame({
                    "base_st": np.arange(ref_reg.start, ref_reg.end),
                    "base_en": np.arange(ref_reg.start + 1, ref_reg.end + 1),
                    "level": levels
                })

                # Prepare the base coordinates dataframe
                base_coords = pd.DataFrame({
                    "x": np.arange(ref_reg.start, ref_reg.end),
                    "y": 1,
                    "base": [base for base in seq],
                    "text_color": [BASE_COLORS[b] for b in seq]
                })

                # Create the plot

                # Plot the signal data
                for sample_name, sample_df in sig_df.groupby('Read'):
                    p.line(x='Reference Position', y='Signal', source=sample_df, line_width=1.5, alpha=0.7, color="red")

                # Add base annotations
                if self.basecall.value:
                    p.add_glyph(ColumnDataSource(base_coords), Text(x="x", y="y", text="base", text_color="text_color", text_font_size="12pt"))

                # Plot the levels as segments
                if self.level.value:
                    p.segment(x0='base_st', x1='base_en', y0='level', y1='level', source=level_df, line_width=2, color='blue', legend_label="Levels")
        except Exception as e: 
            print(e)

        self.figure = pn.Column(p)
        # if isinstance(self.df_annotation, pd.DataFrame):
        #     if len(self.df_annotation) > 0:
        #         df_annotation_active = self.df_annotation[self.df_annotation['summary_state'].isin(self.annotation_widget.value)]
        #         for _, row in df_annotation_active.iterrows():
        #             self.p1.vspan(x=row['acquisition_raw_index'], line_color='gray', line_width=2)
                    
        #         self.figure = pn.Column(self.p1, self.p2, self.rslider, sizing_mode='stretch_width')

    def _setup_layout(self):
        # if len(self.tabs) > 0:
        #     self.tabs[0] = pn.Column(self.figure)
        # else:
        #     self.tabs.append(pn.Column(self.figure))

        self.layout = pn.Row(
            pn.Column(
                pn.WidgetBox(
                    '''
                    # Anchored to Reference Visualization
                    ''',
                    self.reference_widget,
                    self.start,
                    self.end,
                    self.level,
                    self.basecall,
                    # self.start_time_widget,
                    # self.end_time_widget,
                    # widgets.Select(name='Normalization', options=['None', 'Z-Score', 'MAD']),
                    # widgets.Select(name='Segementation', options=['None', 'Ruptures', 'Model']),
                    self.plot_button,
                    self.export_button,
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

class Achovis(Base):
    def _setup_layout(self):
        # if len(self.tabs) > 0:
        #     self.tabs[0] = pn.Column(self.figure)
        # else:
        #     self.tabs.append(pn.Column(self.figure))

        self.layout = pn.Row(
            pn.Column(
                pn.WidgetBox(
                    '''
                    # Anchored to Read Visualization
                    ''',
                    widgets.TextInput(name="Read ID"),
                    # widgets.IntInput(name="Alignment Start"),
                    # widgets.IntInput(name="Alignment End"),
                    self.start_time_widget,
                    self.end_time_widget,
                    widgets.Select(name='Normalization', options=['None', 'Z-Score', 'MAD']),
                    widgets.Select(name='Segementation', options=['None', 'Ruptures', 'Model']),
                    self.plot_button,
                    self.export_button,
                    width=330
                ),
            ),
            self.figure,
            margin=(20, 20, 20, 20)  
        )

class Segment(Base):
    def _setup_layout(self):
        # if len(self.tabs) > 0:
        #     self.tabs[0] = pn.Column(self.figure)
        # else:
        #     self.tabs.append(pn.Column(self.figure))

        self.layout = pn.Row(
            pn.Column(
                pn.WidgetBox(
                    '''
                    # Segmentation and Tail Estimation
                    ''',
                    widgets.TextInput(name="Reference Name"),
                    widgets.IntInput(name="Alignment Start"),
                    widgets.IntInput(name="Alignment End"),
                    # self.start_time_widget,
                    # self.end_time_widget,
                    widgets.Select(name='Normalization', options=['None', 'Z-Score', 'MAD']),
                    widgets.Select(name='Segementation', options=['None', 'Ruptures', 'Model']),
                    self.plot_button,
                    self.export_button,
                    width=330
                ),
            ),
            self.figure,
            margin=(20, 20, 20, 20)  
        )

class Search(Base):
    def _setup_layout(self):
        # if len(self.tabs) > 0:
        #     self.tabs[0] = pn.Column(self.figure)
        # else:
        #     self.tabs.append(pn.Column(self.figure))

        self.layout = pn.Row(
            pn.Column(
                pn.WidgetBox(
                    '''
                    # Sequence Search
                    ''',
                    widgets.TextInput(name="Sequence"),
                    # widgets.IntInput(name="Alignment Start"
                    # self.start_time_widget,
                    # self.end_time_widget,
                    # widgets.Select(name='Normalization', options=['None', 'Z-Score', 'MAD']),
                    # widgets.Select(name='Segementation', options=['None', 'Ruptures', 'Model']),
                    self.plot_button,
                    self.export_button,
                    width=330
                ),
            ),
            self.figure,
            margin=(20, 20, 20, 20)  
        )