import ast
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).parent


@pytest.mark.parametrize("path", sorted(ROOT.glob("*.ipynb")), ids=lambda p: p.name)
def test_notebooks_are_output_free_and_valid_python(path):
    notebook = json.loads(path.read_text())
    assert notebook["nbformat"] == 4
    for index, cell in enumerate(notebook["cells"]):
        assert not cell["metadata"]
        if cell["cell_type"] == "code":
            assert cell["execution_count"] is None
            assert cell["outputs"] == []
            source = "".join(cell["source"])
            ast.parse(source, filename=f"{path.name}:cell{index}")
            assert "get_frozen_credentials" not in source
            assert "AWS_SECRET_ACCESS_KEY" not in source
            assert "AWS_SESSION_TOKEN" not in source


def test_index_includes_adjacent_days_and_uses_half_open_year():
    notebook = json.loads((ROOT / "s3indexing_2014.ipynb").read_text())
    sources = [
        "".join(c["source"]) for c in notebook["cells"] if c["cell_type"] == "code"
    ]
    index_source = next(s for s in sources if "first_day =" in s)
    for year, previous_last_day in ((2013, 366), (2014, 365)):
        namespace = {"year": year}
        exec(index_source, namespace)
        assert namespace["index_days"][0] == (year - 1, previous_last_day)
        assert namespace["index_days"][-1] == (year + 1, 1)
    query = next(s for s in sources if "query['$and']" in s)
    assert "'$lt' : endtime.timestamp" in query


def test_loader_preserves_unmatched_station_and_filters_network_by_value():
    notebook = json.loads((ROOT / "load_anfdata.ipynb").read_text())
    source = "\n".join("".join(c["source"]) for c in notebook["cells"])
    assert 'doc["sta"] = css_sta' in source
    assert 'net != "AK"' in source
    assert "del netlist[0]" not in source
