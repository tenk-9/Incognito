import pandas as pd
import queue
import itertools

from typing import Union


class AttributeLevelRange:
    """
    属性の一般化レベルの範囲を表現するクラス
        min: 最小レベル
        max: 最大レベル
    """

    def __init__(self, min_level: int, max_level: int) -> None:
        self.min_level = min_level
        self.max_level = max_level

    def __contains__(self, level: int) -> bool:
        """
        レベルが範囲内にあるか確認する
        """
        return self.min_level <= level <= self.max_level


class Lattice:
    """
    Latticeを表現するクラス
    Attributes:
        hierarchy_df: 一般化階層の定義df
        nodes: ノードを表すdf
        ndges: エッジを表すdf
        dropped_nodes: 探索によって狩られたノードを表すdf
        dropped_edges: 探索によって狩られたエッジを表すdf
    """

    def __init__(self, hierarchy_df: pd.DataFrame) -> None:
        # init attributes
        ## hierarchy
        self.hierarchy_df = hierarchy_df
        ## edges
        self.edges = pd.DataFrame(columns=["from", "to"])
        self.dropped_edges = pd.DataFrame(columns=["from", "to"])
        ## nodes
        self.num_attributes = self.hierarchy_df["column"].unique().shape[0]
        self._node_df_cols = ["idx"]
        for i in range(self.num_attributes):
            self._node_df_cols.append(f"dim{i + 1}")
            self._node_df_cols.append(f"level{i + 1}")
        self.nodes = pd.DataFrame(columns=self._node_df_cols)
        self.dropped_nodes = pd.DataFrame(columns=self._node_df_cols)

        ## 各属性について、最大の階層を取得
        self.attribute_level_ranges = {}  # 属性名: 階層の最大値
        for col in self.hierarchy_df["column"].unique():
            filtered_hierarchy = self.hierarchy_df[self.hierarchy_df["column"] == col]
            max_level = filtered_hierarchy["parent_level"].max()
            min_level = filtered_hierarchy["parent_level"].min()
            self.attribute_level_ranges[col] = AttributeLevelRange(min_level, max_level)

    # def _dict2tuple(self, d: dict) -> tuple:
    #     '''
    #     d: {idx: index, dim1: level1, dim2: level2, ...}
    #     -> ()
    #     '''
    #     row = []
    #     for key, value in d.items():
    #         if key == 'idx':
    #             continue
    #         row.extend([key, value])

    #     return tuple(row)

    def _append_node(self, node: dict) -> int:
        """
        dict表現のノードをparseしてself.nodesに追加する
        param:
            node: {idx: index, dim1: level1, dim2: level2, ...}
        return:
            index of appended node in self.nodes
        """
        row = []
        # idx項はスキップ: 処理的にここでのidxは意味をなさないので
        if "idx" in node.keys():
            node.pop("idx")

        # listに変換: {dim1: level1, dim2: level2, ...} -> [dim1, level1, dim2, level2, ...]
        for key, value in node.items():
            row.extend([key, value])

        # 追加対象のノードが既存か判断
        query = pd.DataFrame(columns=self._node_df_cols[1:])
        query.loc[0] = row
        prod = pd.merge(self.nodes, query, how="inner")
        if len(prod) == 1:
            # すでに同じノードがあるので、そのidxを返す
            return int(prod.iloc[0]["idx"])
        elif len(prod) > 1:
            # すでに重複したノードが存在しており、何かがおかしい
            raise Exception(f"Duplicate node found in self.nodes: {prod}")
        else:
            # self.nodesに追加
            append_node_idx = len(self.nodes)
            self.nodes.loc[append_node_idx] = [append_node_idx] + row
            return append_node_idx

    def _append_edge(self, from_idx: int, to_idx: int) -> None:
        """
        エッジを張る
        param:
            from_idx: self.nodesにおける、fromノードのindex
            to_idx: self.nodesにおける、toノードのindex
        """
        # 追加対象のエッジが既存か判断
        query = pd.DataFrame(columns=["from", "to"])
        query.loc[0] = [from_idx, to_idx]
        prod = pd.merge(self.edges, query, how="inner")

        # すでに同じエッジがあるので、何もしない
        if len(prod) == 1:
            return
        else:
            # self.edgesに追加
            self.edges.loc[len(self.edges)] = [from_idx, to_idx]

    def construct(self) -> "Lattice":
        """
        latticeを構築する
        """
        bfs_queue = queue.Queue()
        found_node_ids = set()

        # {attribute: level}のdictを見ながらbfsする
        root = {key: 0 for key, value in self.attribute_level_ranges.items()}

        ## 初期化: 一般化レベルが最も低いnodeを追加
        bfs_queue.put(root)
        root_idx = self._append_node(root)
        root["idx"] = root_idx  # 0でないとself.nodesの初期状態が空でないことになる
        assert root_idx == 0
        found_node_ids.add(root_idx)

        # bfsでlatticeを構築
        while not bfs_queue.empty():
            current_node = bfs_queue.get()

            # nodeの属性(key)それぞれについて、一段上の一般化階層があり得るか確認
            for key in current_node.keys():
                if key == "idx":
                    continue  # idx項はスキップ

                # あり得たらnodeを追加、エッジを構築
                if current_node[key] + 1 in self.attribute_level_ranges[key]:
                    # copyして新しいノードを作成
                    new_node = current_node.copy()
                    new_node[key] += 1

                    # append to self.nodes and self.edges
                    new_node["idx"] = self._append_node(new_node)
                    self._append_edge(current_node["idx"], new_node["idx"])

                    ## 既存のノードの時は、探索queueに追加しない
                    if new_node["idx"] not in found_node_ids:
                        bfs_queue.put(new_node)
                        found_node_ids.add(new_node["idx"])
        return self

    def drop_node(self, id: Union[int, float]) -> None:
        """
        idのノードを削除する
        """
        # idのノードを削除
        tar_node = self.nodes[self.nodes["idx"] == id]
        self.dropped_nodes = pd.concat([self.dropped_nodes, tar_node])
        self.nodes = self.nodes[self.nodes["idx"] != id]

        # idのノードに関連するエッジを削除
        self.dropped_edges = pd.concat(
            [
                self.dropped_edges,
                self.edges[(self.edges["from"] == id) | (self.edges["to"] == id)],
            ]
        )
        self.edges = self.edges[(self.edges["from"] != id) & (self.edges["to"] != id)]

    def reconstruct(self, ref_lattice: "Lattice") -> None:
        """
        ref_latticeを参照して、latticeを再構築する
        self.nodesから、ref_lattice.drop_nodesの条件を含むノードを削除し、エッジを再構築
        """
        # self.nodesのうち、ref_lattice.dropped_nodesに含まれる条件を満たす（削除されるべき）ノードを抽出
        dropping_nodes = pd.merge(
            self.nodes,
            ref_lattice.dropped_nodes,
            how="inner",
            on=ref_lattice.dropped_nodes.columns.tolist()[1:],  # idxを除く
        )

        for id in dropping_nodes["idx_x"]:
            # ノードを削除
            self.drop_node(id)

    def merge_with(self, other: "Lattice") -> "Lattice":
        """
        引数に取ったLatticeとselfをマージする
        nodesの直積をとり、エッジを再構築
        新しいLatticeインスタンスを返す
        param other: マージ対象のLattice
        return: 新しいLatticeインスタンス
        """
        new_lattice = Lattice(pd.concat([self.hierarchy_df, other.hierarchy_df], ignore_index=True))
        
        # ノードの直積をとる
        self_nodes = self.nodes.drop(columns=["idx"])  # idxは除外
        other_nodes = other.nodes.drop(columns=["idx"])  # idxは除外
        new_nodes = pd.merge(self_nodes, other_nodes, how="cross")

        node_df_cols = []
        for i in range(len(new_nodes.columns) // 2):
            node_df_cols.append(f"dim{i + 1}")
            node_df_cols.append(f"level{i + 1}")

        new_nodes.columns = node_df_cols
        # idx列を追加
        new_nodes["idx"] = range(len(new_nodes))

        new_lattice.nodes = new_nodes


        # 各ノードペアについて、エッジを再構築
        for node1, node2 in itertools.combinations(new_nodes.index, 2):
            
            continue
        print(f"node1: {new_nodes.loc[node1]}, node2: {new_nodes.loc[node2]}")

    def __str__(self) -> str:
        """
        Latticeの文字列表現
        """
        return (
            f"\nLattice object at {hex(id(self))}\nnodes:\n{self.nodes},\nedges:\n{self.edges}\n"
            ""
        )
