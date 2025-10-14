# Incognito - Python Implementation

k-匿名化アルゴリズム Incognito のPython実装です。

## 概要
Incognitoは、データセットのk匿名性を担保する一般化変換を高速に探索するアルゴリズムです。準識別子の少ない組み合わせから順に検証を行い、k匿名性を満たし得ないパターンを枝刈りすることで探索空間を絞り込み、高速な探索を行います。k-匿名性を満たすすべての一般化変換が得られるため、アプリケーションの目的に合わせて最適な変換を柔軟に選択できます。

### 参考文献
> [LeFevre, K., *et al.*, "Incognito: efficient full-domain K-anonymity." *In Proc. ACM SIGMOD*, 49–60, 2005.](https://dl.acm.org/doi/10.1145/1066157.1066164)

## セットアップ

### 1. Python環境の構築

#### uvを使う場合（推奨）
```bash
# uvのインストール（https://docs.astral.sh/uv/getting-started/installation/）
$ uv sync
```

#### condaを使う場合
```bash
# condaのインストール（https://docs.conda.io/projects/conda/en/latest/user-guide/install/）
$ conda env create -f conda_env.yaml
```

### 2. プログラムの実行

#### 基本的な使い方
```bash
$ uv run python main.py --k 10 --q_cols sex workclass marital-status
```

#### 実行結果
実行すると、`((準識別子名, 変換レベル), ...)`をkey、対応する一般化変換済みのDataFrameをvalueとするdictが返されます。

#### コマンドラインオプション
```
usage: main.py [-h] [--dataset DATASET] [--k K] [--q_cols Q_COLS [Q_COLS ...]] [--verbose] [--dropna]

options:
  -h, --help            ヘルプを表示
  --dataset DATASET     使用するデータセット（デフォルト: 'adult'、現在は'adult'のみ対応）
  --k K                 k-匿名性のパラメータ（デフォルト: 10）
  --q_cols Q_COLS [Q_COLS ...]
                        一般化する準識別子のリスト（例: 'workclass', 'education'）
                        独自の一般化階層を使う場合は末尾に _ を付ける（例: 'workclass_'）
  --verbose             詳細な出力を有効化
  --dropna              NaNを含むレコードを削除
```

## データと一般化階層について

### ディレクトリ構造
```
.
├── .data/
│   ├── .hierarchy/          # 標準の一般化階層定義
│   │   ├── adult_hierarchy_age.csv
│   │   └── adult_hierarchy_workclass.csv
│   ├── sex.txt              # 独自の一般化階層定義
│   └── workClass.txt
├── main.py
├── pyproject.toml
└── src/
    ├── incognito.py
    └── ...
```

### 一般化階層の定義形式

#### 標準形式（`.data/.hierarchy/`内）
セミコロン区切りで、左から右へ一般化の段階を定義します。
```
Private;Non-Government;*
Self-emp-not-inc;Non-Government;*
...
```

#### 独自形式（`.data/`直下）
タブのインデントで階層構造を表現します。
```
Work
    Working
        Individual
            Private
            ...
    Not_working
        Unemployed
            Without-pay
            ...
        outlier
            ?
```

### 独自の一般化階層を使う場合

**利用方法：**
```bash
# 準識別子名の末尾に _ を付けて指定
$ uv run python main.py --k 10 --q_cols sex_ workclass_
```
- 現在利用可能: `workclass_`, `sex_`

**新しい独自階層を追加する場合：**
1. `.data/`ディレクトリにファイルを配置
2. `src/utils.py`の`hierarchy_filepaths`に以下の形式で追加
   ```python
   columnName_: "./data/fileName"
   ```
