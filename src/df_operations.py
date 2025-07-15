import pandas as pd
from typing import List

NAN_REPLACE_DICT = {
    "workclass": "DROP",
}


def generalize(df: pd.DataFrame, hierarchy_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generalize the DataFrame based on the provided hierarchy.

    df: Input DataFrame to be generalized.
    hierarchy_df: DataFrame containing the hierarchy mapping. Each row should be generalization target, and have col, from, to information.
    return: Generalized DataFrame.

    hierarchy_df should be like:
       column        child child_level         parent parent_level
    workclass  Federal-gov           0  In-government            1
    workclass    Local-gov           0  In-government            1
    workclass    State-gov           0  In-government            1
    ...
    """

    generalized_df = df.copy()

    # 各カラムについて、hierarchy_dfによる一般化
    for generalize_col in hierarchy_df["column"].unique():
        # 一般化規約の抽出
        mapping = hierarchy_df[hierarchy_df["column"] == generalize_col][
            ["child", "parent"]
        ]
        # 各列について一般化規則を反映
        merged = generalized_df[[generalize_col]].merge(
            mapping, left_on=generalize_col, right_on="child", how="left"
        )
        generalized_df[generalize_col] = merged["parent"].where(
            merged["parent"].notna(), merged[generalize_col]
        )

    return generalized_df


def replace_nan(
    df: pd.DataFrame, replace_dict: dict = NAN_REPLACE_DICT
) -> pd.DataFrame:
    """
    Replace NaN values in the DataFrame based on the provided dictionary.

    df: Input DataFrame with potential NaN values (raw adult dataset).
    replace_dict: Dict to replace NaN, {col_name: replace_value}. If replace_value is 'DROP', the column will be dropped.
    return: DataFrame with NaN values replaced.
    """

    replaced_df = df.copy()
    drop_cols = [col for col, value in replace_dict.items() if value == "DROP"]
    print(drop_cols)
    replaced_df.dropna(subset=drop_cols, inplace=True)
    replaced_df.fillna(replace_dict, inplace=True)

    return df


def is_k_anonymous(df: pd.DataFrame, target_cols: List[str], k: int) -> bool:
    """
    dfがtarget_colsにおいてk-匿名であるか確認する

    df: Input DataFrame
    target_cols: List of columns to check for k-anonymity.
    k: The value of k for k-anonymity.
    return: True if the DataFrame is k-anonymous, False otherwise.
    """

    # 各target_colsの組み合わせでグループ化し、サイズをカウント
    grouped = df.groupby(target_cols, dropna=False)
    # print(grouped.size())
    # 各グループのサイズがk以上であるか確認
    is_k_anonymous = all(size >= k for size in grouped.size())

    return is_k_anonymous
