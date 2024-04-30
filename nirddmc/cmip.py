import typer
import yaml
from ecgtools import Builder
from ecgtools.parsers.cmip import parse_cmip6_using_directories, parse_cmip5_using_directories
from typing import List
from intake_esm.cat import Aggregation

def cmip(root_paths: List[str] = typer.Option(..., "--root-paths","-rp", 
                                    help='Root path of the CMIP project output.'),
        filename: str = typer.Option(...,"--file-name","-fn",help='Name to use when saving the built catalog'),
        depth: int = typer.Option(4, help='Recursion depth. Recursively walk root_path to a specified depth'),
         
        pick_latest_version: bool = typer.Option(True, help='Whether to only catalog lastest version of data assets or keep all versions'),
        cmip_version: int = typer.Option(default=6, help='CMIP phase (e.g. 5 for CMIP5 or 6 for CMIP6)'),

        exclude_patterns: List[str] = typer.Option(
            default=['*/files/*', '*/latest','.cmorout/*', '*/NorCPM1/*', '*/NorESM1-F/*','*/NorESM2-LM/*','*/NorESM2-MM/*'],    
            help='List of patterns to exclude in the cataloge build'),
        config_filepath: str = typer.Option(None, help='Yaml file to configure build arguments'),
        nthreads: int = typer.Option(1, help='Number of threads to use when building the catalog'),
        compression: bool = typer.Option(False, help='Whether to compress the output csv file'),
        directory: str = typer.Option('./', "--dir", "--directory", help='Where to store created intake catalog'),
        description: str = typer.Option(None, help='Detailed multi-line description to fully explain the collection')):
    if config_filepath:
        with open(config_filepath, 'r') as file:
            config = yaml.safe_load(file)
            root_path = config.get('root_path', root_path)
            depth = config.get('depth', depth)
            pick_latest_version = config.get('pick_latest_version', pick_latest_version)
            cmip_version = config.get('cmip_version', cmip_version)
            filename = config.get('filename', filename)
            exclude_patterns = config.get('exclude_patterns', exclude_patterns)
    
    if cmip_version not in set([5, 6]):
        raise ValueError(f'cmip_version = {cmip_version} is not valid. Valid options include: 5 and 6.')

    builder = Builder(
        paths=root_paths,
        depth=depth,
        exclude_patterns=exclude_patterns,
        joblib_parallel_kwargs={'n_jobs': nthreads, 'verbose':13},    
    )

    if cmip_version == 5:
        builder.build(parsing_func=parse_cmip5_using_directories)    
    elif cmip_version == 6:
        builder.build(parsing_func=parse_cmip6_using_directories)
    
    builder.clean_dataframe()

    aggregations = [Aggregation(attribute_name="variable_id", type = "union")]

    aggregations.append(
        Aggregation(
            attribute_name="time_range",
            type="join_existing",
            options={ "dim": "time", "coords": "minimal", "compat": "override" }
        )
    )

    aggregations.append(
        Aggregation(
            attribute_name= "member_id",
            type= "join_new",
            options={ "coords": "minimal", "compat": "override" }
        )
    )
    groupby_attrs=[
        "activity_id",
        "institution_id"
    ]

    
    if compression:
        builder.save(name=filename,data_format='netcdf', 
                     path_column_name ='path',
                     variable_column_name='variable_id', 
                     to_csv_kwargs=dict(compression='gzip',index=False),
                     catalog_type='file',
                    aggregations=aggregations,
                    directory=directory,
                    groupby_attrs=groupby_attrs) 
    else:
        builder.save(name=filename,data_format='netcdf', 
             path_column_name ='path',
             variable_column_name='variable_id', 
             to_csv_kwargs=dict(index=False), 
             directory=directory, catalog_type='file',
             aggregations=aggregations,
             groupby_attrs=groupby_attrs
        )

