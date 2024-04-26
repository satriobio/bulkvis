import math
import numpy as np
import pandas as pd

import panel as pn
pn.extension('tabulator', design='material', template='material', loading_indicator=True)

import holoviews as hv
import hvplot.pandas

import h5py

@pn.cache
def load_file(file_input):
    if file_input:
        protocol = file_input.split(':')[0]
        if protocol == 'https' or protocol == 's3':
            file = h5py.File(file_input, "r", driver="ros3")
        else:
            file = h5py.File(file_input, "r")
        return file
    return None

@pn.cache
def load_signal(file_input, position, rate, annotation):
    file = load_file(file_input)
    if file:
        if position:
            channel = f"Channel_{position.split(':')[0]}"
            start = int(position.split(':')[1].split('-')[0])
            end = int(position.split(':')[1].split('-')[1])

            if abs(end-start) <= 1000:
                start = math.floor(start * 4000)
                end = math.floor(end * 4000)
                signal = file['Raw'][channel]['Signal'][start:end]
                time = np.arange(start, end)
                df = pd.DataFrame({'Time': time, 'Signal': signal})
                df = df[::rate]
                df['Time'] = df['Time'] / 4000
                return df
    return pd.DataFrame({'Time': [], 'Signal': []})

def load_annotation_list(file_input, position):
    file = load_file(file_input)
    if file:
        for x in file['StateData']:
            path = file['StateData'][x]['States']
            fields = ["acquisition_raw_index", "summary_state"]
            enum_field = 'summary_state'

            data_labels = {}
            for field in fields:
                data_labels[field] = path[field]
            data_dtypes = {}
            if h5py.check_dtype(enum=path.dtype[enum_field]):
                dataset_dtype = h5py.check_dtype(enum=path.dtype[enum_field])
                data_dtypes = {v: k for k, v in dataset_dtype.items()}

            labels_df = pd.DataFrame(list(data_dtypes.items()), columns=['Key', 'Value'])
            return labels_df
    return pd.DataFrame({'Key': [], 'Value': []})

def load_annotation(file_input, position):
    file = load_file(file_input)
    if file:
        if position:
            channel = f"Channel_{position.split(':')[0]}"
            start = int(position.split(':')[1].split('-')[0])
            end = int(position.split(':')[1].split('-')[1])

            path = file['StateData'][channel]['States']
            fields = ["acquisition_raw_index", "summary_state"]
            enum_field = 'summary_state'

            data_labels = {}
            for field in fields:
                data_labels[field] = path[field]
            data_dtypes = {}
            if h5py.check_dtype(enum=path.dtype[enum_field]):
                dataset_dtype = h5py.check_dtype(enum=path.dtype[enum_field])
                data_dtypes = {v: k for k, v in dataset_dtype.items()}

            labels_df = pd.DataFrame(data=data_labels)
            labels_df['state'] = labels_df['summary_state'].map(data_dtypes)
            labels_df.rename(columns={'acquisition_raw_index': 'Time'}, inplace=True)
            labels_df['Time'] = labels_df['Time'] / 4000
            labels_df = labels_df[(labels_df['Time']>= start) & (labels_df['Time'] <= end )]

            return labels_df
    return pd.DataFrame({'Time': [], 'state': []})

def get_lines(df):
    if ~df.empty:
        return hv.Overlay([hv.VLine(time).opts(color='orange', line_dash='dashed') for time in df['Time']] + 
                          [hv.Text(row['Time'], 1500, row['state'], rotation=45, halign='left').opts() for index, row in df.iterrows()]
)
    return hv.Overlay([])

file_select = pn.widgets.FileInput(sizing_mode='stretch_width')
file_input = pn.widgets.TextInput(name='Path', placeholder='Enter a local path or a S3 URI here ...', sizing_mode='stretch_width', value = None)
chanel_input = pn.widgets.TextInput(name='Channel', placeholder='50:88360-88900 ...', sizing_mode='stretch_width', value = None)

annotation_list = hvplot.bind(load_annotation_list, file_input=file_input, position=chanel_input).interactive()
annotation = hvplot.bind(load_annotation, file_input=file_input, position=chanel_input).interactive()
checkbox_input = pn.widgets.CheckBoxGroup(name='Checkbox Group', options=annotation_list['Value'].to_list())
selected_annotation = annotation.pipe(
    lambda df,checkbox_input: df[df['state'].isin(checkbox_input)],checkbox_input
)

summary = annotation.pipe(
    lambda df: df['state'].value_counts().reset_index().rename(columns={'index': 'State', 'state': 'Count'})
)

def a(df):
    return df

table = pn.widgets.Tabulator(annotation)
# table.filters(pn.bind(a))

signal = hvplot.bind(load_signal, file_input=file_input, position=chanel_input, rate=30, annotation=selected_annotation).interactive()
selected_signal = signal.pipe(
    lambda df: df
)

plot = signal.hvplot.line(x='Time', y='Signal', min_height=300, responsive=True, grid=True)
lines = pn.bind(get_lines, selected_annotation)
timeseries = plot * lines
timeseries = timeseries.dmap()

text = """
#  Bulkvis

This application help you work with ONT bulkfile and signal data.
"""

sidebar = pn.layout.WidgetBox(
    pn.pane.Markdown(text, margin=(0, 10)),
    file_input,
    chanel_input,
    checkbox_input,
    max_width=350,
    sizing_mode='stretch_width'
).servable(area='sidebar')

main = pn.Tabs(
    ('Raw Signal', timeseries),
    ('Summary', table),
    sizing_mode='stretch_both', min_height=1000
)

template = pn.template.VanillaTemplate(
    title='Bulkvis',
    sidebar=[sidebar],
    main=main,
)

template.servable()

pn.serve(template, title='Bulkvis', port=8888)
