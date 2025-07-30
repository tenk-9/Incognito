import argparse

from src import utils
from src.incognito import Incognito
from src.utils import vprint

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
        "workclass",
        "sex",
        "education",
        "marital-status",
        # "age",
        "native-country",
    ],
    help="List of quasi-identifier columns to generalize (e.g., 'workclass', 'education'). For use of unofficial hierarchy, put _ at the end of the column name (e.g., 'workclass_').",
)
parser.add_argument(
    "--verbose",
    action="store_true",
    help="Enable verbose output",
)
parser.add_argument(
    "--dropna",
    action="store_true",
    help="Drops records which includes NaN.",
)

args = parser.parse_args()
utils.set_verbose(args.verbose)

# fetch dataset
vprint("Fetching dataset:", args.dataset)
dataset = utils.fetch_dataset(args.dataset)
vprint(f"Dataset fetched: {dataset.shape[0]} records.")

# drop nan if specified
if args.dropna:
    vprint("Dropping records with NaN values...")
    dataset = utils.dropna(dataset)
    vprint(f"Records after dropping NaN: {dataset.shape[0]}")

# read hierarchies definition
vprint(f"Reading generalization hierarchies for {args.q_cols}...")
hierarchy = utils.read_hierarchies_by_col_names(args.q_cols)
# filter hierarchy
hierarchy = hierarchy[(hierarchy["child_level"] == 0)]

# incognito
print(f"Starting Incognito... with k={args.k} and quasi-identifiers: {args.q_cols}")
incognito = Incognito(dataset, hierarchy, args.k)
incognito.run()
incognito.print_result()
if utils.VERBOSE:
    incognito.verify_result()  # dropna後のデータを渡す
