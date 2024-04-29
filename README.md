# Data Management Catalogs for Data on NIRD

The `nirddmc` command line tool is used for creating an "intake-catalog" needed for using `intake-esm`. It allows for efficient and easy browsing and subsetting of the data archive available on NIRD.

## Installation

Clone the repository: 
```
git clone https://github.com/Ovewh/nirddmc.git && cd nirddmc
```

Install the cli:

```
pip install -e .
```


## Usage

To use `nirddmc`, run the following command:

```
nirddmc --help
Usage: nirddmc [OPTIONS]                                                                                                                                                  
                                                                                                                                                                           
 Build a catalog of CMIP data assets                                                                                                                                       
                                                                                                                                                                           
╭─ Options ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *  --root-paths           -rp                              TEXT     Root path of the CMIP project output. [default: None] [required]                                    │
│    --depth                                                 INTEGER  Recursion depth. Recursively walk root_path to a specified depth [default: 4]                       │
│    --pick-latest-version       --no-pick-latest-version             Whether to only catalog lastest version of data assets or keep all versions                         │
│                                                                     [default: pick-latest-version]                                                                      │
│    --cmip-version                                          INTEGER  CMIP phase (e.g. 5 for CMIP5 or 6 for CMIP6) [default: 6]                                           │
│    --csv-filepath                                          TEXT     File path to use when saving the built catalog [default: None]                                      │
│    --exclude-patterns                                      TEXT     List of patterns to exclude in the cataloge build                                                   │
│                                                                     [default: */files/*, */latest, .cmorout/*, */NorCPM1/*, */NorESM1-F/*, */NorESM2-LM/*,              │
│                                                                     */NorESM2-MM/*]                                                                                     │
│    --config-filepath                                       TEXT     Yaml file to configure build arguments [default: None]                                              │
│    --nthreads                                              INTEGER  Number of threads to use when building the catalog [default: 1]                                     │
│    --compression               --no-compression                     Whether to compress the output csv file [default: no-compression]                                   │
│    --install-completion                                             Install completion for the current shell.                                                           │
│    --show-completion                                                Show completion for the current shell, to copy it or customize the installation.                    │
│    --help                                                           Show this message and exit.                                                                         │
```

