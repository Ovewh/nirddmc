import intake
import xarray as xr
import cf_xarray
import pathlib
from ecgtools.parsers.utilities import extract_attr_with_regex
from ecgtools.builder import INVALID_ASSET, TRACEBACK


# Parse CESM PPE file output and return info for creating intake catalog
def parse_cesm_ppe(str: file, dict: xarray_open_kwargs=None) -> dict:
    # Extract attributes from file path
    _default_kwargs = {'engine': 'netcdf4', 'chunks': {}, 'decode_times': True}
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

    try:
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

    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}

# Build CESM PPE intake catalog using the parser function

def cesm_ppe(root_paths: List[str] = typer.Option(..., "--root-paths","-rp", help='Root path of the CESM PPE project output.'),
        depth: int = typer.Option(4, help='Recursion depth. Recursively walk root_path to a specified depth'),
        csv_filepath: str = typer.Option(default=None, help='File path to use when saving the built catalog'),
        exclude_patterns: List[str] = typer.Option(
            default=['*/parameter_262_w_control.nc'],
            help='List of patterns to exclude in the cataloge build'),
        config_filepath: str = typer.Option(None, help='Yaml file to configure build arguments'),
        nthreads: int = typer.Option(1, help='Number of threads to use when building the catalog'),
        compression: bool = typer.Option(False, help='Whether to compress the output csv file')):
    if config_filepath:
        with open(config_filepath, 'r') as file:
            config = yaml.safe_load(file)
            root_path = config.get('root_path', root_path)
            depth = config.get('depth', depth)
            csv_filepath = config.get('csv_filepath', csv_filepath)
            exclude_patterns = config.get('exclude_patterns', exclude_patterns)

    if csv_filepath is None:
        raise ValueError("Please provide csv-filepath. e.g.: './cesm_ppe.csv.gz'")

    builder = Builder(
        paths=root_paths,
        depth=depth,
        exclude_patterns=exclude_patterns,
        joblib_parallel_kwargs={'n_jobs': nthreads, 'verbose':13},
    )

    builder.build(parsing_func=parse_cesm_ppe)

    builder.clean_dataframe()

    df = builder.df
    if compression:
        df.to_csv(csv_filepath, compression='gzip', index=False)
    else:
        df.to_csv(csv_filepath, index=False)

    return df