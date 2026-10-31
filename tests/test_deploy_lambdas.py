def test_identical_code(deploy_tool):
    assert deploy_tool.classify_drift("x = 1\n", "x = 1\n") == deploy_tool.IDENTICAL


def test_line_endings_are_not_drift(deploy_tool):
    assert deploy_tool.classify_drift("x = 1\r\n", "x = 1\n") == deploy_tool.IDENTICAL


def test_black_reformat_is_not_drift(deploy_tool):
    deployed = "foo = {'a':1}\n"
    repo = 'foo = {"a": 1}\n'
    assert deploy_tool.classify_drift(deployed, repo) == deploy_tool.FORMATTING_ONLY


def test_changed_logic_is_drift(deploy_tool):
    assert deploy_tool.classify_drift("x = 1\n", "x = 2\n") == deploy_tool.CODE_DRIFT


def test_unparseable_deployed_code_is_drift(deploy_tool):
    assert deploy_tool.classify_drift("def (:\n", "x = 1\n") == deploy_tool.CODE_DRIFT


def test_extras_ignore_the_handler_folders_and_bytecode(deploy_tool):
    names = [
        "lambda_function.py",
        "__pycache__/lambda_function.cpython-314.pyc",
        "requests/",
        "requests/api.py",
    ]
    assert deploy_tool.find_extras(names, "lambda_function.py") == ["requests/api.py"]


def test_layer_packages_reads_dist_info(deploy_tool):
    names = [
        "python/pandas/__init__.py",
        "python/pandas-2.3.3.dist-info/METADATA",
        "python/pandas-2.3.3.dist-info/RECORD",
        "python/pyarrow-22.0.0.dist-info/METADATA",
    ]
    assert deploy_tool.layer_packages(names) == ["pandas==2.3.3", "pyarrow==22.0.0"]
