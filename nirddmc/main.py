import typer

from .cesm_ppe import cesm_ppe
from .cmip import cmip
from .merge_catalogs import merge_cmip_catalogs

app = typer.Typer(help="Managment of intake catalogs for accessing data on NIRD")

app.command(help='Merge a list of cmip catalogs')(merge_cmip_catalogs)
app.command(help='Build a catalog of CMIP data assets')(cmip)
app.command(help='Build a catalog of CESM PPE data assets')(cesm_ppe)
if __name__ == "__main__":
    app()