from ucimlrepo import fetch_ucirepo
import pandas as pd
import os


def fetch_dataset() -> pd.DataFrame:
    """
    fetch adult dataset from UCI Machine Learning Repository (https://archive.ics.uci.edu/dataset/2/adult)
    RETURN: adult dataset as a pandas dataframe
    """
    # Fetch dataset
    if os.path.exists(".data/adult.csv"):
        data = pd.read_csv(".data/adult.csv", index_col=0)
    else:
        adult = fetch_ucirepo(id=2)
        data = adult.data.original
        data.columns = adult.data.headers
        data.to_csv(".data/adult.csv")
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
