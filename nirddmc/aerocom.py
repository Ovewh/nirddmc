import typer
import yaml
import ecgtools
from ecgtools import Builder
from ecgtools.builder import INVALID_ASSET, TRACEBACK
import pathlib
import typer
from typing import List
from intake_esm.cat import Aggregation

def aerocom_parser(file: str) -> dict:
    # Path to be parsed: 
    # /lustre/storeB/project/aerocom/aerocom-users-database/DURF/histSST-dust-glb/NorESM2-LM_histSST_DURF-dust-glb/aerocom3_NorESM2-LM-histSST_DURF-dust-glb_ztp_Column_1949_monthly.nc


    try:
        experiment_stream = {
            'glb': 'histSST-dust-glb',
            'glb-piaer': 'histSST-dust-glb-piaer',
            'histSST' : 'histSST',
            'spt' : 'histSST-dust-spt',
        }
        file = pathlib.Path(file)
        filename = file.name
        info = {}
        filename_parts = filename.split('_')
        path_parts = file.split('/')
        info['experiment'] = experiment_stream.get(path_parts[-3], path_parts[-3])
        info['model'] = path_parts[-2].split('_')[0]
        info['variable'] = filename_parts[-4]
        info['coordinate'] = filename_parts[-3]
        info['frequency'] = filename_parts[-1].split('.')[0]
        info['time'] = filename_parts[-2]
        info['path'] = str(file)
        return info
    except Exception as e:
        return {INVALID_ASSET: file, TRACEBACK: str(e)}
    
def aerocom(
    root_path: List[str] = typer.Argument(...,"-rp", help="Root paths to the directory containing the Aerocom data to be parsed"),
    depth: int = typer.Option(2,"--depth", '-d', help="Depth of the directory tree to be parsed"),
    nthreads: int = typer.Option(2, "--nthreads", "-nt", help="Number of threads to use for parsing"),
    compression: bool = typer.Option(False, "--compression", "-c", help="Whether to compress the output csv file"),
    directory: str = typer.Option('./', "--dir", "--directory", help="Where to store the created intake catalog"),
    catalog_name: str = typer.Option('aerocom', "--catalog-name", "-cn", help="Name to use when saving the built catalog")):

    builder = Builder(
        paths=root_path,
        depth=depth,
        joblib_parallel_kwargs={'n_jobs': nthreads, 'verbose': 13}
    )
    builder.build(parsing_func=aerocom_parser)
    builder.clean_dataframe()

    aggregations = [Aggregation(attribute_name="variable", type="union")]
    aggregations.append(
        Aggregation(attribude_name="time", type="join_existing", 
                    options={"dim": "time", "coords": "minimal", "compat": "override"})
    )
    groupby_attrs=[
        "experiment",
        "frequency",
        "model",
        "coordinate"
    ]

    if compression:
        builder.save(name=catalog_name, data_format='netcdf', 
                     path_column_name='path',
                     variable_column_name='variable',
                     to_csv_kwargs=dict(compression='gzip', index=False),
                     catalog_type='file',
                     directory=directory,
                     aggregations=aggregations,
                     groupby_attrs=groupby_attrs)
    else:
        builder.save(name=catalog_name, data_format='netcdf', 
                     path_column_name='path',
                     variable_column_name='variable',
                     catalog_type='file',
                     directory=directory,
                     aggregations=aggregations,
                     groupby_attrs=groupby_attrs)