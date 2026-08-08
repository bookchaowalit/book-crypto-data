import py_compile
from pathlib import Path
import unittest


class SmokeTest(unittest.TestCase):
    def test_package_compiles(self):
        root = Path(__file__).parents[1] / "src" / "book_crypto"
        for path in root.glob("*.py"):
            py_compile.compile(str(path), doraise=True)


if __name__ == "__main__":
    unittest.main()
