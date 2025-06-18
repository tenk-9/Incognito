from ucimlrepo import fetch_ucirepo
import pandas as pd

def fetch_dataset() -> pd.DataFrame:
    """
    fetch adult dataset from UCI Machine Learning Repository (https://archive.ics.uci.edu/dataset/2/adult)
    RETURN: adult dataset as a pandas dataframe
    """
    # Fetch dataset
    adult = fetch_ucirepo(id=2) 
    data = adult.data.original
    data.columns = adult.data.headers
    return data

def fetch_dataset_csv() -> None:
    """
    save adult dataset as a csv file
    """
    adult = fetch_dataset()
    adult.to_csv('adult.csv')

def read_hierarchy(file_path: str) -> dict:
    """
    階層定義ファイルを読み、下位階層から上位階層への逆引きリンク集を返す
    param file_path: path to the hierarchy file
    return: hierarchy as a dictionary

    read txt:
	Human
		Male
		Female

    return dict:
    {
        'Human': 'ROOT',
        'Male': 'Human',
        'Female': 'Human'
    }
    """
    def _parse_hierarchy(line: str) -> tuple:
        """
        parse a line of hierarchy and return its level and value
        param line: a line of hierarchy, contains few \t before the value
        return: (level, value)
        """
        nest_level = 0
        for c in line:
            if c == '\t':
                nest_level += 1
            else:
                break
        value = line.strip()
        return nest_level, value

    # read hierarchy from file
    hierarchy = {}
    parents = [] # parents[i]: the last-checked i-th nested value
    with open(file_path, 'r') as f:
        # ルートを定義、初期化
        prev_level, prev_value = 0, 'ROOT'
        parents.append(prev_value)

        for line in f:
            # read hierarchy line
            level, value = _parse_hierarchy(line)

            # 初めて見る階層の場合はメモに追加、既知レベルの場合は値を更新
            if len(parents) == level:
                parents.append(value)
            else:
                parents[level] = value
            
            # 直前の行より階層が深いときは、逆引きリンクを張る
            if level > prev_level:
                hierarchy[value] = prev_value
            # 直前の行と同階層の時は、メモしてあった一つ上階層の親へリンクを張る
            elif level == prev_level:
                hierarchy[value] = parents[level - 1]
            else: # 直前の行より階層が浅いときは、親を見つけてリンクを張り、メモを更新
                hierarchy[value] = parents[level - 1]
                parents[level] = value

            prev_level, prev_value = level, value

    return hierarchy

def read_hierarchy_df(file_path: str) -> pd.DataFrame:
    """
    read_hierarchy()の戻り値をdatafremeにするwrapper
    param file_path: path to the hierarchy file
    return: hierarchy as a pandas DataFrame
    
    return DataFrame:
    child, parent
    ...  , ...
    ...  , ...
    ...

    """
    hierarchy = read_hierarchy(file_path)
    return pd.DataFrame(list(hierarchy.items()), columns=['child', 'parent'])
