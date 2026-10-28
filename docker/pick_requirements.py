"""Print the pyproject.toml requirement lines of the named packages.

The lean images (duckdb_server, backup) install only a few of the project's
dependencies. Reading their version ranges from pyproject.toml keeps the versions
in one place. Fails when a name is not a dependency, so a rename cannot silently
drop a package from an image.

Usage: python pick_requirements.py pyproject.toml duckdb python-dotenv > requirements.txt
"""

import re
import sys
import tomllib


def main() -> None:
    pyproject, *names = sys.argv[1:]
    with open(pyproject, "rb") as f:
        dependencies = tomllib.load(f)["project"]["dependencies"]
    by_name = {re.split(r"[\s<>=!~;\[]", dep, maxsplit=1)[0].lower(): dep for dep in dependencies}
    missing = [name for name in names if name.lower() not in by_name]
    if missing:
        sys.exit(f"not in [project].dependencies of {pyproject}: {', '.join(missing)}")
    for name in names:
        print(by_name[name.lower()])


if __name__ == "__main__":
    main()
