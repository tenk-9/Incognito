import argparse

import utils
from incognito import Incognito
from utils import vprint

# parse command line arguments
parser = argparse.ArgumentParser(description="Run Incognito with specified parameters.")
parser.add_argument(
    "--dataset",
    type=str,
    default="adult",
    help="Dataset to use (default: 'adult'). Currently only 'adult' is supported.",
)
parser.add_argument(
    "--k",
    type=int,
    default=10,
    help="k-anonymity parameter (default: 20)",
)
parser.add_argument(
    "--q_cols",
    type=str,
    nargs="+",
    default=[
        "workclass_",
        "sex",
        "education",
    ],
    help="List of quasi-identifier columns to generalize (e.g., 'workclass', 'education'). For use of unofficial hierarchy, put _ at the end of the column name (e.g., 'workclass_').",
)
parser.add_argument(
    "--verbose",
    action="store_true",
    help="Enable verbose output",
)

args = parser.parse_args()
utils.set_verbose(args.verbose)

# fetch dataset
vprint("Fetching dataset:", args.dataset)
dataset = utils.fetch_dataset(args.dataset)

# read hierarchies definition
vprint(f"Reading generalization hierarchies for {args.q_cols}...")
hierarchy = utils.read_hierarchies_by_col_names(args.q_cols)
# filter hierarchy
hierarchy = hierarchy[(hierarchy["child_level"] == 0)]

# incognito
vprint("Starting Incognito...")
incognito_result = Incognito(dataset, hierarchy, args.k)
if utils.VERBOSE:
    incognito_result.verify_result()
