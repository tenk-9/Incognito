from typing import List
import pandas as pd
import queue

import df_operations
from lattice import Lattice
from utils import vprint


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

    def _node_to_generalization_tuples(
        self, node, hierarchy: pd.DataFrame
    ) -> List[tuple]:
        """
        ノードを一般化条件のタプルのリストに変換する
        param node: pandasの行オブジェクト (cols: idx, dim1, level1, dim2, level2, ...)
        param hierarchy: 一般化階層の定義df
        return: 一般化条件のタプルのリスト [(dim1, level1), (dim2, level2), ...]
        """
        num_attributes = len(hierarchy["column"].unique())
        conditions = []
        for i in range(1, num_attributes + 1):
            dim = getattr(node, f"dim{i}")
            level = getattr(node, f"level{i}")
            conditions.append((dim, level))
        return conditions

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
        # TODO: priningもBFSで
        for node in lattice.nodes.itertuples():
            conditions = self._node_to_generalization_tuples(node, hierarchy)

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
            vprint(f"Constructing Lattice for {hierarchy['column'].unique()}")
            lattice = Lattice(hierarchy).construct()

            # 変換Latticeの各ノードについて、k匿名性を確認し、枝刈りを行う
            lattice = self._pruning(lattice, df, hierarchy, k)

            return lattice

        # 対象の属性が複数ある場合は、分割して再探索
        else:
            attr_count = len(hierarchy["column"].unique())
            attribute1 = hierarchy["column"].unique()[: attr_count // 2]
            attribute2 = hierarchy["column"].unique()[attr_count // 2 :]
            vprint(f"Dividing into {attribute1}, {attribute2}")

            # 枝刈り済みのLatticeを取得
            prunded_lattice1 = self._incognito(
                df, hierarchy[hierarchy["column"].isin(attribute1)], k
            )
            prunded_lattice2 = self._incognito(
                df, hierarchy[hierarchy["column"].isin(attribute2)], k
            )

            # 一旦複数属性のLatticeを作成
            ## TODO: ここでLatticeを生成してから枝刈りするのは遠回りの処理なので、prunded_lattice1とprunded_lattice2を直接マージして複数属性のLatticeを構築したい
            vprint(
                f"Consttructing Lattice for {hierarchy['column'].unique()}", end=" -> "
            )
            lattice = Lattice(hierarchy).construct()
            vprint(lattice.nodes.shape[0], "nodes")

            prunded_lattice1.merge_with(prunded_lattice2)

            # 各属性の枝刈り済みLatticeをもとに、複数属性のLatticeを枝刈り
            vprint(f"Pruning by {attribute1}", end=" -> ")
            lattice.reconstruct(prunded_lattice1)
            vprint(lattice.nodes.shape[0], "nodes")
            vprint(f"Pruning by {attribute2}", end=" -> ")
            lattice.reconstruct(prunded_lattice2)
            vprint(lattice.nodes.shape[0], "nodes")

            # 再度枝刈り
            ## たとえば下のような例があるので、Workclassについてk-10匿名を満たしても、二つの属性を組み合わせると満たさなくなる
            ## ある属性がk匿名を満たさないなら、その上位集合はk匿名を満たさない　が、ある属性が満たすとき、上位集合も満たすとは限らない
            # Never-worked      Female        3
            #                   Male          7
            vprint("Pruning by k-anonymity", end=" -> ")
            lattice = self._pruning(lattice, df, hierarchy, k)
            vprint(lattice.nodes.shape[0], "nodes")

            return lattice

    def _print_result(self) -> None:
        """
        結果を表示する
        """
        print(f"\nIncognito result:")
        print(
            f"There are {len(self.result_lattice.nodes)} combinations of generalization levels satisfying k-anonymity (k={self.k}):"
        )
        for i, node in enumerate(self.result_lattice.nodes.itertuples()):
            conditions = self._node_to_generalization_tuples(node, self.hierarchies)
            conditions_str = ", ".join(f"{dim}={level}" for dim, level in conditions)
            print(i + 1, conditions_str)
        print()

    def verify_result(self) -> bool:
        """
        処理後の結果の検証を行う
        return: 検証結果 (True: 正常, False: 異常)
        """
        print("Verifying Incognito result...")
        for node in self.result_lattice.nodes.itertuples():
            conditions = self._node_to_generalization_tuples(node, self.hierarchies)

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
            num_dims = self.result_lattice.num_attributes
            conditions_tup = [
                f"{getattr(node, f'dim{i}')}={getattr(node, f'level{i}')}"
                for i in range(1, num_dims + 1)
            ]
            print(f"node: {', '.join(conditions_tup)}")
            if not df_operations.is_k_anonymous(
                generalized_df, [str(dim) for dim, _ in conditions], self.k, debug=True
            ):
                print(f"-does not satisfy k-anonymity (k={self.k}).")
                return False
            else:
                print(f"-satisfies k-anonymity (k={self.k}).")
            print()

        print(f"All {len(self.result_lattice.nodes)} nodes satisfy k-anonymity.")
        return True


from node import Node
import itertools


class Lattice_:
    def __init__(self, hierarchy: pd.DataFrame) -> None:
        """
        Q: 準識別子のリスト
        """
        self.nodes: List["Node"] = []
        self.Q: List[str] = hierarchy["column"].unique().tolist()
        self.hierarchy: pd.DataFrame = hierarchy
        self.attributes: int = 0

    def _single_attribute_initialization(self) -> None:
        """
        単一属性の一般化について初期化、Incognitoの初期条件
        """
        # 単一属性について一般化変換を構築
        for q in self.Q:
            tmp_nodes = []
            # 一般化の定義を取得
            generalizations = self.hierarchy[self.hierarchy["column"] == q]
            max_generalization_level = generalizations["parent_level"].max()
            for generalization_level in range(max_generalization_level + 1):
                # ノードを生成
                node = Node({q: generalization_level})
                tmp_nodes.append(node)
            # 親子関係を構築
            for i in range(
                1, len(tmp_nodes)
            ):  # rootはそこに至るノードがないのでスキップ
                # それ以外は前のノードをfromにする
                tmp_nodes[i].add_src_node(tmp_nodes[i - 1])
                tmp_nodes[i - 1].add_dst_node(tmp_nodes[i])

            self.nodes.extend(tmp_nodes)

    def _node_generation(self) -> None:
        """
        属性数+1のノードを生成する
        """
        # i個の属性があるとき、i-1個目までの属性とレベルが同じ かつ i個目の属性が左<右
        active_nodes = []
        new_nodes = []
        for node in self.nodes:
            if not node.deleted:
                active_nodes.append(node)

        for p, q in itertools.combinations(active_nodes, 2):
            # for p in range(len(active_nodes)):
            #     for q in range(p+1, len(active_nodes)):
            # p_attr = active_nodes[p].generalization
            # q_attr = active_nodes[q].generalization
            # p_attr_keys = list(p_attr.keys())
            # q_attr_keys = list(q_attr.keys())
            p_attr_keys = list(p.generalization.keys())
            q_attr_keys = list(q.generalization.keys())
            if len(p_attr_keys) == 1 and len(q_attr_keys) == 1:
                # 単一属性の比較
                if p_attr_keys[0] != q_attr_keys[0]:
                    new_nodes.append(Node(p.generalization | q.generalization))
            else:
                if (
                    p_attr_keys[:-1] == q_attr_keys[:-1]
                    and p_attr_keys[-1] < q_attr_keys[-1]
                ):
                    new_nodes.append(Node(p.generalization | q.generalization))
        self.nodes = new_nodes
        print(len(self.nodes), "nodes generated.")

    def increment_attributes(self) -> None:
        """
        属性の数を1増やす
        """
        if self.attributes == 0:
            self._single_attribute_initialization()
            self.attributes += 1
        else:
            # 複数属性の一般化について初期化
            # GraphGeneration

            # node_generation
            self._node_generation()

            # TODO: edge generation

            raise NotImplementedError(
                "Multiple attribute generalization is not implemented yet."
            )


class Incognito_:
    def __init__(self, T: pd.DataFrame, hierarchy: pd.DataFrame, k: int) -> None:
        self.T: pd.DataFrame = T  # 対象のテーブル
        self.Q: List[str] = hierarchy["column"].unique().tolist()  # 準識別子のリスト
        self.hierarchy: pd.DataFrame = hierarchy  # 一般化階層の定義df
        self.k: int = k  # k-匿名性のk値
        self.lattice: Lattice_  # 構築済みのLattice

    def run(self) -> pd.DataFrame:
        """
        Incognitoの実行
        return: 一般化されたDataFrame
        """
        self.lattice = Lattice_(self.hierarchy)
        self.lattice.increment_attributes()  # initialization of the lattice
        priority_queue = queue.PriorityQueue()

        # 属性の組み合わせ数をボトムアップしていく
        for attributes in range(len(self.Q)):
            # nodeの高さによる優先度付きqueue
            for node in self.lattice.nodes:
                ## rootを流し込んで初期化
                if node.is_root() and not node.deleted:
                    priority_queue.put(node)

            while not priority_queue.empty():
                node = priority_queue.get()

                # k匿名を満たすとしてマークされていたら、スキップ
                if node.is_marked() or node.deleted:
                    continue
                else:
                    # nodeに定義された一般化変換に従い、一般化を実施、k匿名性を検証
                    # ノードの一般化変換を取得: level-0 -> level-n
                    def row_match(row):
                        # すべての(dim, level)条件を満たすか
                        result = any(
                            (row["column"] == dim)
                            and (row["child_level"] == 0)
                            and (row["parent_level"] == level)
                            for dim, level in node.generalization.items()
                        )
                        return result

                    valid_generalization = self.hierarchy[
                        self.hierarchy.apply(row_match, axis=1)
                    ]
                    # 一般化を適用
                    generalized_df = df_operations.generalize(
                        self.T, valid_generalization
                    )
                    # k匿名性の確認
                    k_anonymous = df_operations.is_k_anonymous(
                        generalized_df, list(node.generalization.keys()), self.k
                    )

                    # k匿名性を満たすなら、ノードとその直親をマーク
                    if k_anonymous:
                        node.mark()
                        for dst_node in node.to_nodes:
                            dst_node.mark()

                    # k匿名でないとき、一段上のノードを優先度付きqueueに追加
                    else:
                        for dst_node in node.to_nodes:
                            priority_queue.put(dst_node)
                        node.delete()
            # attributesを増やし、Latticeを更新
            self.lattice.increment_attributes()
