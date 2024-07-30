import pytest
from flask import Flask
from flask.testing import FlaskClient, FlaskCliRunner
from google.cloud import firestore
from main import app as main_app


@pytest.fixture()
def app(mocker) -> Flask:

    mocker.patch.object(firestore.Client, "__init__")

    app = main_app
    app.config.update({
        "TESTING": True,
    })

    # other setup
    with app.app_context():
        pass

    yield app

    # other teardown


@pytest.fixture()
def client(app: Flask) -> FlaskClient:
    return app.test_client()


@pytest.fixture()
def runner(app: Flask) -> FlaskCliRunner:
    return app.test_cli_runner()

