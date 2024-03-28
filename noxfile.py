import os

import nox
from nox import Session
from nox.command import CommandFailed

PYTHON_BASE_VERSION = "3.8"
PYTHON_VERSIONS = ("3.8", "3.9", "3.10", "3.11", "3.12")
AUTOFLAKE_VERSION = "2.2.1"
RUFF_VERSION = "0.2.2"
MYPY_VERSION = "1.8.0"
BUILD_VERSION = "1.0.3"
TWINE_VERSION = "4.0.2"

SOURCE = "flexplan"
SOURCE_DIR = SOURCE
NOXFILE_PATH = "noxfile.py"
TEST_DIR = "tests"


@nox.session(python=False)
def shell_completion(session: Session):
    shell = os.getenv("SHELL")
    if shell is None or "bash" in shell:
        session.run("echo", 'eval "$(register-python-argcomplete nox)"')
    elif "zsh" in shell:
        session.run("echo", "autoload -U bashcompinit")
        session.run("echo", "bashcompinit")
        session.run("echo", 'eval "$(register-python-argcomplete nox)"')
    elif "tcsh" in shell:
        session.run("echo", "eval `register-python-argcomplete --shell tcsh nox`")
    elif "fish" in shell:
        session.run("echo", "register-python-argcomplete --shell fish nox | .")
    else:
        session.run("echo", 'eval "$(register-python-argcomplete nox)"')


@nox.session(python=False)
def clean(session: Session):
    session.run(
        "rm",
        "-rf",
        ".mypy_cache",
        ".pytype",
        ".pytest_cache",
        ".pytype_output",
        "build",
        "dist",
        "html_cov",
        "html_doc",
        "logs",
    )
    session.run(
        "sh",
        "-c",
        "find . | grep -E '(__pycache__|\.pyc|\.pyo$$)' | xargs rm -rf",
    )


@nox.session(python=PYTHON_BASE_VERSION, reuse_venv=True)
@nox.parametrize("autoflake", [AUTOFLAKE_VERSION])
@nox.parametrize("ruff", [RUFF_VERSION])
def format(session: Session, autoflake: str, ruff: str):
    session.install(
        f"autoflake~={autoflake}",
        f"ruff~={ruff}",
    )
    try:
        session.run("taplo", "fmt", "pyproject.toml", external=True)
    except CommandFailed:
        session.warn(
            "Seems that `taplo` is not found, skip formatting `pyproject.toml`. "
            "(Refer to https://taplo.tamasfe.dev/ for information on how to install "
            "`taplo`)"
        )
    session.run("autoflake", "--version")
    session.run("autoflake", SOURCE_DIR, NOXFILE_PATH, TEST_DIR)
    session.run("ruff", "--version")
    session.run(
        "ruff",
        "check",
        "--select",
        "I",
        "--fix",
        SOURCE_DIR,
        NOXFILE_PATH,
        TEST_DIR,
    )
    session.run("ruff", "format", SOURCE_DIR, NOXFILE_PATH, TEST_DIR)


@nox.session(python=PYTHON_BASE_VERSION, reuse_venv=True)
@nox.parametrize("autoflake", [AUTOFLAKE_VERSION])
@nox.parametrize("ruff", [RUFF_VERSION])
def lint(session: Session, autoflake: str, ruff: str):
    session.install(
        f"autoflake~={autoflake}",
        f"ruff~={ruff}",
    )
    try:
        session.run("taplo", "check", "pyproject.toml", external=True)
    except CommandFailed:
        session.warn(
            "Seems that `taplo` is not found, skip checking `pyproject.toml`. "
            "(Refer to https://taplo.tamasfe.dev/ for information on how to install "
            "`taplo`)"
        )
    session.run("autoflake", "--version")
    session.run("autoflake", "--check-diff", SOURCE_DIR, NOXFILE_PATH, TEST_DIR)
    session.run("ruff", "--version")
    session.run(
        "ruff",
        "check",
        "--select",
        "I",
        "--diff",
        SOURCE_DIR,
        NOXFILE_PATH,
        TEST_DIR,
    )
    session.run(
        "ruff",
        "format",
        "--check",
        "--diff",
        SOURCE_DIR,
        NOXFILE_PATH,
        TEST_DIR,
    )
