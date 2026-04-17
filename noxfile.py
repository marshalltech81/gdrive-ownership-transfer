import nox

nox.options.sessions = ["lint", "format", "typecheck", "tests", "bandit"]
nox.options.reuse_existing_virtualenvs = True

PYTHON_VERSIONS = ["3.11", "3.12", "3.13"]


@nox.session(python=PYTHON_VERSIONS[0])
def lint(session: nox.Session) -> None:
    session.run("uv", "run", "ruff", "check", ".", external=True)


@nox.session(python=PYTHON_VERSIONS[0])
def format(session: nox.Session) -> None:
    session.run("uv", "run", "ruff", "format", "--check", ".", external=True)


@nox.session(python=PYTHON_VERSIONS[0])
def typecheck(session: nox.Session) -> None:
    session.run("uv", "run", "mypy", "src", external=True)


@nox.session(python=PYTHON_VERSIONS)
def tests(session: nox.Session) -> None:
    session.run("uv", "run", "pytest", external=True)


@nox.session(python=PYTHON_VERSIONS[0])
def bandit(session: nox.Session) -> None:
    session.run("uv", "run", "bandit", "-q", "-r", "src", external=True)
