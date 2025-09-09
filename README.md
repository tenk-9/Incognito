# 環境構築
## ディレクトリ構造の確認・データの準備
ディレクトリ構成は次のようです：
```
.
├── .data
│   ├── .hierarchy
│   │   ├── adult_hierarchy_age.csv
│   │   ├── ...
│   │   └── adult_hierarchy_workclass.csv
│   ├── sex.txt
│   └── workClass.txt
├── .gitignore
├── .python-version
├── README.md
├── main.py
├── pyproject.toml
├── src
│   ├── .data -> ../.data/
│   ├── __init__.py
│   ├── df_operations.py
│   ├── incognito.py
│   ├── lattice.py
│   ├── node.py
│   ├── test.py
│   └── utils.py
└── uv.lock
```
- `.data/`: 独自の一般化階層定義を置いてください。タブの深さで階層を表現するようにしてください。
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
- `.data/.hierarchy/`: 一般化階層の定義を置いてください。各属性値について、`;`区切りで左から右へ一般化した値を列挙してください。
  ```
    Private;Non-Government;*
    Self-emp-not-inc;Non-Government;*
    ...
  ```
## python環境の構築
1. パッケージ管理ツールをインストール
   - pythonのパッケージ管理には`uv`を使います
   - https://docs.astral.sh/uv/getting-started/installation/#__tabbed_1_1 からインストールしてください　
2. python環境を構築
   - 環境を構築します: `uv syc`
# プログラムの実行
1. `uv run python main.py`でプログラムが実行されます。
   - コマンドライン引数でいくつかのオプションが指定できます:
        ```
        usage: main.py [-h] [--dataset DATASET] [--k K] [--q_cols Q_COLS [Q_COLS ...]] [--verbose] [--dropna]

        Run Incognito with specified parameters.

        options:
        -h, --help            show this help message and exit
        --dataset DATASET     Dataset to use (default: 'adult'). Currently only 'adult' is supported.
        --k K                 k-anonymity parameter (default: 10)
        --q_cols Q_COLS [Q_COLS ...]
                                List of quasi-identifier columns to generalize (e.g., 'workclass', 'education'). For use of unofficial hierarchy, put _ at the end of the column name (e.g., 'workclass_').
        --verbose             Enable verbose output
        --dropna              Drops records which includes NaN.
        ```
    - 例）準識別子: `sex, workclass, marital-status`、`k=10`の場合は、以下のコマンドで実行できます。
        ```
        uv run python main.py --k 10 --q_cols sex workclass marital-status
        ```
    - `.data/`直下に配置した独自の一般化階層の定義を指定するときは、`--q_cols`で指定する識別子の後ろに`_`をつけてください。
      - 現在は`workclass`と`sex`のみ独自の一般化階層が利用可能です。
      - 新規で独自の一般化階層を定義する場合は、以下のとおりにしてください。
        1. `.data/`にファイルを配置する
        2. `src/utils.py`の`hierarchy_filepaths`に、`columnName_: "./data/fileName"` の形で記述してください
2. `((準識別子名, 変換レベル), ...)`をkey、対応する一般化変換済みのdataframeをvalueとするdictが返されます。