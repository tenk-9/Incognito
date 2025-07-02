import pandas as pd
import queue

class Lattice():
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
        self.edges = pd.DataFrame(columns=['from', 'to'])
        self.dropped_edges = pd.DataFrame(columns=['from', 'to'])
        ## nodes
        dimensions = self.hierarchy_df['column'].unique().shape[0]
        self._node_df_cols = ['idx']
        for i in range(dimensions):
            self._node_df_cols.append(f'dim{i+1}')
            self._node_df_cols.append(f'level{i+1}')
        self.nodes = pd.DataFrame(columns=self._node_df_cols)
        self.dropped_nodes = pd.DataFrame(columns=self._node_df_cols)

        ## 各属性について、最大の階層を取得
        attribute_maxlevel = {} # 属性名: 階層の最大値
        for col in self.hierarchy_df['column'].unique():
            filterd_hierarchy = self.hierarchy_df[self.hierarchy_df['column'] == col]
            max_level = filterd_hierarchy['parent_level'].max()
            attribute_maxlevel[col] = max_level
        
        # latticeを構築
        self._construct_lattice(attribute_maxlevel)

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
        if 'idx' in node.keys(): node.pop('idx')

        # listに変換: {dim1: level1, dim2: level2, ...} -> [dim1, level1, dim2, level2, ...]
        for key, value in node.items():
            row.extend([key, value])
        
        # 追加対象のノードが既存か判断
        query = pd.DataFrame(columns=self._node_df_cols[1:])
        query.loc[0] = row
        prod = pd.merge(self.nodes, query, how='inner')
        if len(prod) == 1:
            # すでに同じノードがあるので、そのidxを返す
            return int(prod.iloc[0]['idx'])
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
        query = pd.DataFrame(columns=['from', 'to'])
        query.loc[0] = [from_idx, to_idx]
        prod = pd.merge(self.edges, query, how='inner')

        # すでに同じエッジがあるので、何もしない
        if len(prod) == 1:
            return
        else:
            # self.edgesに追加
            self.edges.loc[len(self.edges)] = [from_idx, to_idx]
    
    def _construct_lattice(self, attribute_maxlevel: dict) -> None:
        """
        latticeを構築する
        param:
            attribute_maxlevel: 各属性の最大一般化レベルのdict
                例: {attribute: max_level, attribute2: max_level2, ...}
        """
        bfs_queue = queue.Queue()
        found_node_ids = set()

        # attribute: levelのdictを見ながらbfsする
        root = {key: 0 for key in attribute_maxlevel.keys()}

        ## 初期化: 一般化レベルが最も低いnodeを追加
        bfs_queue.put(root)
        root_idx = self._append_node(root)
        root['idx'] = root_idx # 0でないとself.nodesの初期状態が空でないことになる
        assert root_idx == 0
        found_node_ids.add(root_idx)

        # bfsでlatticeを構築
        while not bfs_queue.empty():
            current_node = bfs_queue.get()
            
            # nodeの属性(key)それぞれについて、一段上の一般化階層があり得るか確認
            for key in current_node.keys(): 
                if key == 'idx': continue # idx項はスキップ
                
                # あり得たらnodeを追加、エッジを構築
                if current_node[key] + 1 <= attribute_maxlevel[key]:
                    # copyして新しいノードを作成
                    new_node = current_node.copy()
                    new_node[key] += 1

                    # append to self.nodes and self.edges
                    new_node['idx'] = self._append_node(new_node)
                    self._append_edge(current_node['idx'], new_node['idx'])

                    ## 既存のノードの時は、探索queueに追加しない
                    if new_node['idx'] not in found_node_ids:
                        bfs_queue.put(new_node)
                        found_node_ids.add(new_node['idx'])
        
        print(self.nodes)
        print(self.edges)

    def drop_node(self, id: int) -> None:
        """
        idのノードを削除する
        """
        # self.nodesから行を削除
        # 削除した行を self.dropped_nodes に追加
        # self.edgesから、id番のノードがfromのエッジを削除
        pass

    def reconstruct(self, ref_lattice: "Lattice") -> None:
        """
        ref_latticeを参照して、latticeを再構築する
        self.nodesから、ref_lattice.drop_nodesの条件を含むノードを削除し、エッジを再構築
        """
        # self.nodesと、ref_lattice.dropped_nodesを内部結合
        # 浮き出たノードをself.dropped_nodesに追加
        # 浮き出たノードに関連するエッジを削除
        pass