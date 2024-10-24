import json
import os
from pathlib import Path

from main import run_single_threaded
from docs import load_index_html


def test_run():
    base_path = Path(__file__).parent / 'dbt-project'
    try:
        run_single_threaded(['build'], base_path)
    except RuntimeError as e:
        text = 'dbt failure: '
        message = e.__str__()
        assert message.startswith(text)
        nodes = json.loads(message[len(text):])
        nodes = [node[:-2] for node in nodes]
        assert nodes == [
            'test.test.failing_test......................................fail[1]  in 0.0',
            'test.test.warning_test......................................warn[1]  in 0.0'
        ]


def test_docs(dbt_docs_bucket):
    base_path = Path(__file__).parent / 'dbt-project'
    catalog_path = base_path / 'target' / 'catalog.json'
    if catalog_path.exists():
        os.remove(catalog_path)
    run_single_threaded(['docs', 'generate'], base_path)
    index_html = load_index_html()
    assert len(index_html) == 1_916_602
