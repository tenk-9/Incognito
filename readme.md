.data
    adultデータセットの生CSV
    各準識別子の一般化階層情報
adultデータセット:　https://archive.ics.uci.edu/dataset/2/adult




conda create -n incognito
conda activate incognito
conda install python=3.12
pip install ucimlrepo


uv init .
uv sync
uv add 
uv pip sync requirements.txt
. .venv/bin/activate