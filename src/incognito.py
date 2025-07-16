from typing import List
import pandas as pd

import utils, df_operations
from lattice import Lattice


class Incognito:
    def __init__(self, df: pd.DataFrame, hierarchies: pd.DataFrame, k: int) -> None:
        """
        Incognitoの初期化
        param df: 入力データフレーム
        param hierarchies: 各属性に対する一般化階層のリストの集合のdf
        param k: k-匿名性のk値

        param hierarchies should be formatted as concatnation of utils.read_hierarchy(), like:
           column        child child_level         parent parent_level
        workclass  Federal-gov           0  In-government            1
        workclass    Local-gov           0  In-government            1
        workclass    State-gov           0  In-government            1
              sex       Female           0          Human            1
        ...
        """
        self.df = df
        self.hierarchies = hierarchies
        self.k = k

        self.result_lattice = self._incognito(self.df, self.hierarchies, self.k)

        self._print_result()

    def _pruning(
        self, lattice: Lattice, df: pd.DataFrame, hierarchy: pd.DataFrame, k: int
    ) -> Lattice:
        """
        Latticeの枝刈りを行う
        param lattice: 枝刈り対象のLattice
        param df: 処理対象のdf (概念上の空参照)
        param hierarchy: 一般化階層の定義df
        param k: k-匿名性のk値 (概念上の空参照)
        return: 枝刈り済みのLattice
        """
        num_attributes = len(hierarchy["column"].unique())
        # 各ノードについて、k匿名性を確認し、枝刈りを行う
        for node in lattice.nodes.itertuples():
            conditions = []
            for i in range(1, num_attributes + 1):
                dim = getattr(node, f"dim{i}")
                level = getattr(node, f"level{i}")
                conditions.append((dim, level))

            # ノードの一般化変換を取得: level-0 -> level-n
            def row_match(row):
                # すべての(dim, level)条件を満たすか
                return any(
                    (row["column"] == dim)
                    and (row["child_level"] == 0)
                    and (row["parent_level"] == level)
                    for dim, level in conditions
                )

            generalize_hierarchy = hierarchy[hierarchy.apply(row_match, axis=1)]

            # 一般化変換
            generalized_df = df_operations.generalize(df, generalize_hierarchy)

            # k匿名性の確認
            if df_operations.is_k_anonymous(
                generalized_df, [str(dim) for dim, _ in conditions], k
            ):
                # k匿名な場合は、枝刈りをしない
                continue
            else:
                # Latticeの枝刈り
                lattice.drop_node(node.idx)

        return lattice

    def _incognito(self, df: pd.DataFrame, hierarchy: pd.DataFrame, k: int) -> Lattice:
        """
        Incognitoのメイン処理
        param df: 処理対象のdf (概念上の空参照)
        param hierarchy: 一般化階層の定義df
        param k: k-匿名性のk値 (概念上の空参照)
        return: 構成済みのLattice
        """

        # 対象の属性が1つなら、一般化してLatticeの枝切りを行う
        if len(hierarchy["column"].unique()) == 1:
            lattice = Lattice(hierarchy).construct()

            # 変換Latticeの各ノードについて、k匿名性を確認し、枝刈りを行う
            lattice = self._pruning(lattice, df, hierarchy, k)

            return lattice

        # 対象の属性が複数ある場合は、分割して再探索
        else:
            attr_count = len(hierarchy["column"].unique())
            attribute1 = hierarchy["column"].unique()[: attr_count // 2]
            attribute2 = hierarchy["column"].unique()[attr_count // 2 :]

            # 枝刈り済みのLatticeを取得
            prunded_lattice1 = self._incognito(
                df, hierarchy[hierarchy["column"].isin(attribute1)], k
            )
            prunded_lattice2 = self._incognito(
                df, hierarchy[hierarchy["column"].isin(attribute2)], k
            )

            # 一旦複数属性のLatticeを作成
            ## TODO: ここでLatticeを生成してから枝刈りするのは遠回りの処理なので、prunded_lattice1とprunded_lattice2を直接マージして複数属性のLatticeを構築したい
            lattice = Lattice(hierarchy).construct()

            # 各属性の枝刈り済みLatticeをもとに、複数属性のLatticeを枝刈り
            lattice.reconstruct(prunded_lattice1)
            lattice.reconstruct(prunded_lattice2)

            # 再度枝刈り
            ## たとえば下のような例があるので、Workclassについてk-10匿名を満たしても、二つの属性を組み合わせると満たさなくなる
            ## ある属性がk匿名を満たさないなら、その上位集合はk匿名を満たさない　が、ある属性が満たすとき、上位集合も満たすとは限らない
            # Never-worked      Female        3
            #                   Male          7
            lattice = self._pruning(lattice, df, hierarchy, k)

            return lattice

    def _print_result(self) -> None:
        """
        結果を表示する
        """
        print(f"\nIncognito result:")
        print(
            f"There are {len(self.result_lattice.nodes)} combinations of generalization levels satisfying k-anonymity (k={self.k}):"
        )
        print(self.result_lattice)

    def verify_result(self) -> bool:
        """
        処理後の結果の検証を行う
        return: 検証結果 (True: 正常, False: 異常)
        """
        for node in self.result_lattice.nodes.itertuples():
            conditions = []
            for i in range(1, len(self.result_lattice._node_df_cols[1:]) // 2 + 1):
                dim = getattr(node, f"dim{i}")
                level = getattr(node, f"level{i}")
                conditions.append((dim, level))

            # ノードの一般化変換を取得
            def row_match(row):
                return any(
                    (row["column"] == dim)
                    and (row["child_level"] == 0)
                    and (row["parent_level"] == level)
                    for dim, level in conditions
                )

            generalize_hierarchy = self.hierarchies[
                self.hierarchies.apply(row_match, axis=1)
            ]

            # 一般化変換
            generalized_df = df_operations.generalize(self.df, generalize_hierarchy)

            # k匿名性の確認
            num_dims = len(self.result_lattice._node_df_cols[1:]) // 2
            conditions_tup = [
                f"{getattr(node, f'dim{i}')}={getattr(node, f'level{i}')}"
                for i in range(1, num_dims + 1)
            ]
            print(f"Checking node: {', '.join(conditions_tup)}")
            if not df_operations.is_k_anonymous(
                generalized_df, [str(dim) for dim, _ in conditions], self.k, debug=True
            ):
                print(f"does not satisfy k-anonymity (k={self.k}).")
                return False
            else:
                print(f"satisfies k-anonymity (k={self.k}).")

        print("All nodes satisfy k-anonymity.")
        return True
