
import os
from pathlib2 import Path

import pytest

BASE_PATH = Path(os.path.abspath(__file__)).parent
CASE_PATH = Path(BASE_PATH, "tests/*")

if __name__ == '__main__':
    pytest.main(['-m ', 'ess and not lifecycle_need_speedup', '-n 10', '--reruns 3', CASE_PATH])
