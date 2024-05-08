import pathlib
from typing import List

import cf_xarray
import intake
import typer
import xarray as xr
from ecgtools import Builder
from ecgtools.builder import INVALID_ASSET, TRACEBACK
from intake_esm.cat import Aggregation


# Parse CESM PPE file output and return info for creating intake catalog
def parse_cesm_ppe(file: str, xarray_open_kwargs: dict=None) -> dict:
    # Extract attributes from file path
    _default_kwargs = {'engine': 'netcdf4', 'chunks': {}, 'decode_times': True}
    try:
        if xarray_open_kwargs:
            xarray_open_kwargs.update(_default_kwargs)
        else:
            xarray_open_kwargs = _default_kwargs
        experiment_stream = {
            'PD': 'present-day',
            'PI': 'pre-industrial'
        }
        frequency_stream = {
            'h0': 'monthly',
            'h1': 'daily',
        }

        file = pathlib.Path(file)
        info = {}


        #filename cc_PPE_250_ensemble_PD.131.h0.IWC.nc
        filename = file.name
        filename_parts = file.stem.split('_')[-1].split('.')
        info['experiment'] = experiment_stream.get(filename_parts[0], filename_parts[0].lower)
        info['ensemble'] = int(filename_parts[1])
        info['frequency'] = frequency_stream.get(filename_parts[2], filename_parts[2])
        info['variable'] = filename_parts[3]
        with xr.open_dataset(file, **xarray_open_kwargs) as ds:
            info['units'] = ds[info['variable']].attrs.get('units')
            info['long_name'] = ds[info['variable']].attrs.get('long_name')
            info['vertical_levels'] = 1
            # get the beginning of the time series as a string and store in info 
            start_time = ds.time.dt.strftime('%Y-%m-%d').values[0]
            info['start_time'] = start_time
            end_time = ds.time.dt.strftime('%Y-%m-%d').values[-1]
            info['end_time'] = end_time
            date_range = f'{start_time}-{end_time}'
            info['time_range'] = date_range
            try:
                info['vertical_levels'] = ds[ds.cf['vertical'].name].size
            except (KeyError, AttributeError, ValueError):
                pass
        info['path'] = str(file)
        return info
    except Exception as e:
        return {INVALID_ASSET: file, TRACEBACK: str(e)}


# Build CESM PPE intake catalog using the parser function

def cesm_ppe(
    root_path: List[str] = typer.Option(..., "--root-path", "-rp", help='Root path to the directory containing the CESM PPE files'),
    depth: int = typer.Option(2, help='Recursion depth. Recursively walk root_path to a specified depth'),
    filename: str = typer.Option(...,"--file-name","-fn",help='Name to use when saving the built catalog'),
    nthreads: int = typer.Option(2, help='Number of threads to use when building the catalog'),
    compression: bool = typer.Option(False, help='Whether to compress the output csv file'),
    directory: str = typer.Option('./', "--dir", "--directory", help='Where to store created intake catalog'),
    description: str = typer.Option(None, help='Detailed multi-line description to fully explain the collection')):
    
    builder = Builder(
        paths=root_path,
        depth=depth,
        
        joblib_parallel_kwargs={'n_jobs': nthreads, 'verbose':13},
    )
    builder.build(parsing_func=parse_cesm_ppe)
    builder.clean_dataframe()

    aggregations = [Aggregation(attribute_name="variable", type = "union")]

    aggregations.append(
        Aggregation(
            attribute_name="time_range",
            type="join_existing",
            options={ "dim": "time", "coords": "minimal", "compat": "override" }
        )
    )

    aggregations.append(
        Aggregation(
            attribute_name= "ensemble",
            type= "join_new",
            options={"dim": "ensamble", "coords": "minimal", "compat": "override" }
        )
    )
    groupby_attrs=[
        "experiment",
        "frequency",
    ]

    if compression:
        builder.save(name=filename, data_format='netcdf', 
                     path_column_name ='path',
                     variable_column_name='variable', 
                     to_csv_kwargs=dict(compression='gzip',index=False),
                     catalog_type='file',
                     description=description,
                        directory=directory,
                     aggregations=aggregations,
                     groupby_attrs=groupby_attrs)
    else:
        builder.save(name=filename, data_format='netcdf', 
                     path_column_name ='path',
                     variable_column_name='variable', 
                     catalog_type='file',
                     description=description,
                     directory=directory,
                     aggregations=aggregations,
                     groupby_attrs=groupby_attrs)