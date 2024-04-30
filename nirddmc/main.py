import typer
from .merge_catalogs import merge_cmip_catalogs
from .cmip import cmip

app = typer.Typer(help="Managment of intake catalogs for accessing data on NIRD")

app.command(help='Merge a list of cmip catalogs')(merge_cmip_catalogs)
app.command(help='Build a catalog of CMIP data assets')(cmip)

if __name__ == "__main__":
    app()