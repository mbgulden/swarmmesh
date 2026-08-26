import sys
from unittest.mock import patch
from swarmmesh.cli import main

def test_cli_status(capsys):
    with patch.object(sys, 'argv', ['swarmmesh', 'status']):
        main()
    captured = capsys.readouterr()
    assert "Mesh status: OK" in captured.out

def test_cli_publish(capsys):
    with patch.object(sys, 'argv', ['swarmmesh', 'publish', 'mytopic', 'mydata']):
        main()
    captured = capsys.readouterr()
    assert "Published to mytopic: mydata" in captured.out
