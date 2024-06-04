import pandas as pd
import panel as pn
import holoviews as hv

from bulkvis.utils.readsignal import ReadSignal 

signal = ReadSignal()
df_signal = pd.DataFrame({"Time": [], "Signal": []})
plot_curve = hv.Curve(df_signal, kdims="Time", vdims="Signal")
pane_plot_curve = pn.pane.HoloViews(plot_curve, sizing_mode="stretch_both")
obj_summary = pn.pane.JSON({}, name="JSON", sizing_mode="stretch_both")

def load_plot():
    df_signal =  signal.get_df()
    overlays = []
    obj_context = {}
    if signal.df_annotation_label is None:
        checkbox_annotation.options = []

    if signal.obj_context:
        obj_context = signal.obj_context
    if signal.is_bulk:
        df_annotation = signal.df_annotation[
            signal.df_annotation["state"].isin(checkbox_annotation.value)
        ]
        if ~df_annotation.empty:
            overlays = overlays + [
                hv.VLine(time).opts(color="orange", line_dash="dashed")
                for time in df_annotation["Time"]
            ] + [
                hv.Text(
                    row["Time"], 1500, row["state"], rotation=45, halign="left"
                ).opts()
                for index, row in df_annotation.iterrows()
            ]
        checkbox_annotation.options = signal.df_annotation_label["Value"].to_list()

    overlays = hv.Overlay(overlays)
    pane_plot_curve.object = (
        hv.Curve(df_signal, kdims="Time", vdims="Signal") * overlays
    )
    obj_summary.object = (obj_context)

def call_load_signal(self):
    signal.read_signal(input_signal.value, input_key.value)
    load_plot()

def call_load_plot(self):
    load_plot()

## MARKDOWN
text_introduction = """
# Bulkvis

Visualise squiggle data from Oxford Nanopore Technologies (ONT) signal files.

Squiggle Data:
"""

text_annotation = """
States Annotations:
"""

## INPUT
input_signal = pn.widgets.TextInput(
    name="Signal File (.fast5/.pod5)",
    placeholder="Enter a local path or a public S3 URI here ...",
    sizing_mode="stretch_width",
    value=None,
)
input_key = pn.widgets.TextInput(
    name="Position or Read ID",
    placeholder="Enter Channel:Start-End or Read Id ...",
    sizing_mode="stretch_width",
    value=None,
)
input_alignment = pn.widgets.TextInput(
    name="Alignment File (.bam)",
    placeholder="Enter a local path or a public S3 URI here ...",
    sizing_mode="stretch_width",
    value=None,
    disabled=True,
)
button_load_data = pn.widgets.Button(
    name="Reload Data", button_type="primary", sizing_mode="stretch_width"
)
checkbox_annotation = pn.widgets.CheckBoxGroup(name="Checkbox Group", options=[])
button_load_plot = pn.widgets.Button(
    name="Reload Plot", button_type="primary", sizing_mode="stretch_width"
)
button_save_signal = pn.widgets.FileDownload(
    button_type="primary",
    sizing_mode="stretch_width",
    label="Save Squiggle",
    callback=pn.bind(signal.get_fast5),
    filename="signal.fast5",
)

button_load_data.on_click(call_load_signal)
button_load_plot.on_click(call_load_plot)

holder = pn.pane.Markdown(text_introduction, margin=(0, 10))

## LAYOUT

sidebar = pn.layout.WidgetBox(
    pn.pane.Markdown(text_introduction, margin=(0, 10)),
    input_signal,
    input_key,
    button_load_data,
    pn.pane.Markdown(text_annotation, margin=(0, 10)),
    checkbox_annotation,
    button_load_plot,
    button_save_signal,
    max_width=350,
    sizing_mode="stretch_width",
).servable(area="sidebar")

main = pn.Tabs(
    ("Raw Squiggle", pane_plot_curve),
    ("File Summary", obj_summary),
    sizing_mode="stretch_both",
)

template = pn.template.MaterialTemplate(
    title="Bulkvis",
    sidebar=[sidebar],
    main=main,
)

template.servable()

import argparse
from typing import Any

class View:
    _help = "Run web app."
    _cli = [
        
    ]

    def run(parser: argparse.ArgumentParser, args: argparse.ArgumentParser, extras: list[Any]) -> int:
        pn.serve(template, title="Bulkvis", port=8888)