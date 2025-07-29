from ucimlrepo import fetch_ucirepo
import pandas as pd
import os

hierarchy_filepaths = {
    "workclass_": ".data/workClass.txt",
    "sex_": ".data/sex.txt",
    "age": ".data/.hierarchy/adult_hierarchy_age.csv",
    "education": ".data/.hierarchy/adult_hierarchy_education.csv",
    "marital-status": ".data/.hierarchy/adult_hierarchy_marital-status.csv",
    "native-country": ".data/.hierarchy/adult_hierarchy_native-country.csv",
    "occupation": ".data/.hierarchy/adult_hierarchy_occupation.csv",
    "race": ".data/.hierarchy/adult_hierarchy_race.csv",
    "salary-class": ".data/.hierarchy/adult_hierarchy_salary-class.csv",
    "sex": ".data/.hierarchy/adult_hierarchy_sex.csv",
    "workclass": ".data/.hierarchy/adult_hierarchy_workclass.csv",
}

def set_verbose(verbose: bool) -> None:
    """
    Set the global verbose flag for logging.
    """
    global VERBOSE
    VERBOSE = verbose

def vprint(*args, **kwargs) -> None:
    """
    Print messages only if verbose mode is enabled.
    """
    if VERBOSE:
        print(*args, **kwargs)

def fetch_dataset(dataset_name: str = "adult") -> pd.DataFrame:
    """
    fetch adult dataset from UCI Machine Learning Repository (https://archive.ics.uci.edu/dataset/2/adult)
    RETURN: adult dataset as a pandas dataframe
    """
    if dataset_name not in [
        "adult",
    ]:
        raise ValueError(f"Unknown dataset name: {dataset_name}")

    # Fetch dataset
    if os.path.exists(f".data/{dataset_name}.csv"):
        data = pd.read_csv(f".data/{dataset_name}.csv", index_col=0)
    else:
        adult = fetch_ucirepo(id=2)
        data = adult.data.original
        data.columns = adult.data.headers
        data.to_csv(f".data/{dataset_name}.csv")
    return data


def fetch_dataset_csv() -> None:
    """
    save adult dataset as a csv file
    """
    adult = fetch_dataset()
    adult.to_csv("adult.csv")


def read_hierarchy(file_path: str, col_name: str) -> pd.DataFrame:
    """
    階層定義ファイルを読み、下位階層から上位階層への逆引きリンク集を返す
    param file_path: path to the hierarchy file
    return: hierarchy df

    read txt:
        Human
                Male
                Female

    return df:
       column        child child_level         parent parent_level
    workclass  Federal-gov           0  In-government            1
    workclass    Local-gov           0  In-government            1
    workclass    State-gov           0  In-government            1
    """

    def _parse_hierarchy(line: str) -> tuple:
        """
        parse a line of hierarchy and return its level and value
        param line: a line of hierarchy, contains few \t before the value
        return: (level, value)
        """
        nest_level = -1
        for c in line:
            if c == "\t":
                nest_level += 1
            else:
                break
        value = line.strip()
        return nest_level, value

    def _format_df(hierarchy_df: pd.DataFrame) -> pd.DataFrame:
        """
        delete links to ROOT and correct the hierarchy levels
        """
        # ROOTを削除
        hierarchy_df = hierarchy_df[hierarchy_df["parent"] != "ROOT"]

        # levelを反転, bottom: 0, top: max_level
        max_level = hierarchy_df[["child_level", "parent_level"]].max().max()
        hierarchy_df.loc[:, "child_level"] = max_level - hierarchy_df["child_level"]
        hierarchy_df.loc[:, "parent_level"] = max_level - hierarchy_df["parent_level"]

        return hierarchy_df

    # read hierarchy from file
    hierarchy_df = pd.DataFrame(
        columns=["child", "child_level", "parent", "parent_level"]
    )
    parents = []  # parents[i]: the last-checked i-th nested value
    with open(file_path, "r") as f:
        # ルートを定義、初期化
        prev_level, prev_value = -1, "ROOT"
        parents.append((prev_value, prev_level))

        for line in f:
            # read hierarchy line
            nest_level, value = _parse_hierarchy(line)
            append_hierarchy = []

            # 初めて見る階層の場合はメモに追加
            if parents[-1][1] < nest_level:
                parents.append((value, nest_level))
            # 既知レベルの場合は家系が変わるので、メモの値をを更新
            else:
                parents[nest_level + 1] = (value, nest_level)

            # メモしてあった直系の先祖へのリンクをすべて張る
            for i in range(nest_level + 1):
                append_hierarchy.append((value, nest_level, *parents[i]))

            # update hierarchy df
            hierarchy_df = pd.concat(
                [
                    hierarchy_df,
                    pd.DataFrame(append_hierarchy, columns=hierarchy_df.columns),
                ],
                ignore_index=True,
            )

            prev_level, prev_value = nest_level, value

    # format df: remove path to ROOT and correct generalization level
    hierarchy_df = _format_df(hierarchy_df)

    # add target column name
    hierarchy_df["column"] = col_name

    return hierarchy_df


def read_hierarchy_official_csv(file_path: str, col_name: str) -> pd.DataFrame:
    """
    read hierarchy from official csv file
    param file_path: path to the hierarchy csv file
    return: hierarchy df
    """
    csv = pd.read_csv(file_path, sep=";", header=None)
    hierarchy_df = pd.DataFrame(
        columns=["child", "child_level", "parent", "parent_level"]
    )
    for child_col in range(csv.shape[1] - 1):
        for parent_col in range(child_col + 1, csv.shape[1]):
            csvf = csv.iloc[:, [child_col, parent_col]]
            csvf = csvf.drop_duplicates()
            append_df = pd.DataFrame(csvf.values, columns=["child", "parent"])
            append_df["child_level"] = child_col
            append_df["parent_level"] = parent_col
            hierarchy_df = pd.concat([hierarchy_df, append_df], ignore_index=True)
   
    # csvとdatatable上のcol名が違うものは置換する
    if col_name == 'salary-class':
        col_name = 'income'
        
    hierarchy_df["column"] = col_name
    return hierarchy_df


def read_hierarchies_by_col_names(col_names: list[str]) -> pd.DataFrame:
    """
    read hierarchies from files by column names
    param col_names: list of column names to read hierarchies
    return: concatenated hierarchy df
    """
    hierarchies = []
    for col_name in col_names:
        if col_name in hierarchy_filepaths:
            if col_name.endswith("_"):
                # unofficialな階層定義ファイルの場合
                hierarchies.append(
                    read_hierarchy(hierarchy_filepaths[col_name], col_name[:-1])
                )
            else:
                # officialな階層定義ファイルの場合
                hierarchies.append(
                    read_hierarchy_official_csv(hierarchy_filepaths[col_name], col_name)
                )
        else:
            raise ValueError(f"Unknown column name: {col_name}")
    return pd.concat(hierarchies, ignore_index=True)
