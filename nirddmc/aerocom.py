import typer
import yaml
import ecgtools
from ecgtools import Builder
from ecgtools.builder import INVALID_ASSET, TRACEBACK


def aerocom_parser