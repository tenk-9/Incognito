from typing import List
import pandas as pd

import utils, df_operations
from lattice import Lattice

# TODO: 探索latticeの表現方法をどうしようか


class Incognito():
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

    def _incognito(self, df: pd.DataFrame, hierarchy: pd.DataFrame, k: int) -> Lattice:
        """
        Incognitoのメイン処理
        param hierarchy: 一般化階層の定義df
        return: k-匿名化されたデータフレーム
        """


        #　対象の属性が1つなら、一般化してLatticeの枝切りを行う
        if len(hierarchy['column'].unique()) == 1:
            lattice = Lattice(hierarchy)

            # 変換Latticeの各ノードについて、k匿名性を確認し、枝刈りを行う
            for node in lattice.nodes.itertuples():
                # ノードの一般化変換を取得: level-0 -> level-n
                generalize_hierarchy = hierarchy[
                    (hierarchy['column'] == node.dim1) &
                    (hierarchy['child_level'] == 0) &
                    (hierarchy['parent_level'] == node.level1)
                ]

                # 一般化変換
                generalized_df = df_operations.generalize(self.df, generalize_hierarchy)

                # k匿名性の確認
                if df_operations.is_k_anonymous(generalized_df, [str(node.dim1)], self.k):
                    # k匿名な場合は、枝刈りをしない
                    break
                else:
                    # Latticeの枝刈り
                    lattice.drop_node(node.idx)

            # 枝刈り済みのLatticeを返す
            return lattice
        
        # 対象の属性が複数ある場合は、分割して再探索
        else:
            attr_count = len(hierarchy['column'].unique())
            attribute1 = hierarchy['column'].unique()[:attr_count//2]
            attribute2 = hierarchy['column'].unique()[attr_count//2:]
            
            # 枝刈り済みのLatticeを取得
            prunded_lattice1 = self._incognito(self.df, hierarchy[hierarchy['column'].isin(attribute1)], self.k)
            prunded_lattice2 = self._incognito(self.df, hierarchy[hierarchy['column'].isin(attribute2)], self.k)

            # 一旦複数属性のLatticeを作成
            ## TODO: ここでLatticeを生成してから枝刈りするのは遠回りの処理なので、prunded_lattice1とprunded_lattice2を直接マージして複数属性のLatticeを構築したい
            lattice = Lattice(hierarchy)

            # 各属性の枝刈り済みLatticeをもとに、複数属性のLatticeを枝刈り
            lattice.reconstruct(prunded_lattice1)
            lattice.reconstruct(prunded_lattice2)

            # 枝刈り済みのLatticeを返す
            return lattice
    
    def _print_result(self) -> None:
        """
        結果を表示する
        """
        print(f"\nIncognito result:")
        print(f"There are {len(self.result_lattice.nodes)} combinations of generalization levels satisfying k-anonymity (k={self.k}):")
        print(self.result_lattice.nodes)
