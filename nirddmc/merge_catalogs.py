import typer
import pandas as pd
import intake
from typing import List
import intake_esm

def _join_tables_from_filepath(filepaths: List[str]):
    dfs = [pd.read_csv(filepath) for filepath in filepaths]
    df = pd.concat(dfs, ignore_index=True)
    df['version']=df['version'].str.slice(start=1).astype(int)
    return df

def _join_tables_from_intake_urls(urls: List[str]):
    dfs = [intake.open_esm_datastore(url).df for url in urls]
    df = pd.concat(dfs, ignore_index=True)
    return df


def merge_cmip_catalogs(catalogs: List[str] = typer.Option(..., "--catalogs","-c", help='List of catalogs to merge'),
        name: str = typer.Option('merge_cmip', help='what to name the merged catalog'),
        outdir: str = typer.Option('./', help='Directory to save the merged catalog'),
        compression: bool = typer.Option(False, help='Whether to compress the output csv file'),
        url_remote_tables: List[str] = typer.Option(None, help='List of url to intake cataloges from remote sources to join with local tables, e.g. pangeo: https://storage.googleapis.com/cmip6/pangeo-cmip6.json'),
        table_id: str = typer.Option(None, help='Identifier to use when saving the merged catalog'),
        metadata_file: str = typer.Option(None, help='File path to yml file containing metadata for the merged catalog')):
    combined_local_table = _join_tables_from_filepath(catalogs)
    if url_remote_tables:
        remote_tables = _join_tables_from_intake_urls(url_remote_tables)
        # remove entries in the remote table that are already in the local table
        keys = [key 
            for key in local_tab.columns.values 
            if key in remote_tab.columns.values
            ]
        i1 = local_tab.set_index(keys).index
        i2 = remote_tab.set_index(keys).index

        remote_filtered = remote_tab[~i2.isin(i1)]
        merged_tab = pd.concat([local_tab, remote_filtered], ignore_index=True)

    else:
        merged_tab = combined_local_table
    
    merged_tab["format"]="netcdf"
    merged_tab.loc[pd.isna(merged_tab["path"]),"format"]="zarr"
    merged_tab.loc[pd.isna(merged_tab["time_range"]),"time_range"]="*"

    merged_tab.loc[pd.isna(merged_tab["path"]),"path"]=merged_tab["zstore"]

    del merged_tab["zstore"]

    merged_tab = merged_tab.drop_duplicates(subset=["path","version"])
    merge_catalogs = {'table_id': table_id, 'esemcat_version':'0.1.0'}
    if metadata_file:
        with open(metadata_file, 'r') as file:
            metadata = yaml.safe_load(file)
            merge_catalogs.update(metadata)

    attributes=[]
    directory_structure_template="mip_era/activity_id/institution_id/source_id/experiment_id/member_id/table_id/variable_id/grid_label/version"

    for att in directory_structure_template.split('/'):
        attributes.append(
            dict(column_name=att,
                vocabulary=f"https://raw.githubusercontent.com/WCRP-CMIP/CMIP6_CVs/master/CMIP6_{att}.json"
                )
        )
    merge_catalog["attributes"]=attributes  

    assets={
        "column_name": "path",
        "format_column_name" : "format"
    }
    merge_catalog["assets"]=assets

    aggregation_control=dict(
    variable_column_name="variable_id",
    groupby_attrs=[
        "activity_id",
        "institution_id"
    ]
    )

    aggregation_control["aggregations"]=[dict(
        attribute_name="variable_id",
        type="union"
    )]

    aggregation_control["aggregations"].append(
        dict(
            attribute_name="time_range",
            type="join_existing",
            options={ "dim": "time", "coords": "minimal", "compat": "override" }
        )
    )

    aggregation_control["aggregations"].append(
        dict(
            attribute_name= "member_id",
            type= "join_new",
            options={ "coords": "minimal", "compat": "override" }
        )
    )

    merge_catalog["aggregation_control"]=aggregation_control
    esm_merged = intake.open_esm_datastore({'esmcat':merge_catalog, 'df': merged_tab})


    esm_merged.serialize(name=name, directory=outdir, catalog_type="file")
