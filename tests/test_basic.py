from flask.testing import FlaskClient
import TagEngineStoreHandler as tesh


def test_home(client: FlaskClient, mocker):
    # Mock session / authentication
    with client.session_transaction() as session:
        # set a user id without going through the login route
        session["credentials"] = 1
        session["user_email"] = "test@test.com"

    # Mock external calls
    mocker.patch.object(tesh.TagEngineStoreHandler, "read_default_settings",
                        return_value=(True, {
                            "template_id": 1,
                            "template_project": "test-project",
                            "template_region": "nowhere",
                            "service_account": "sa@test.com",
                        }))

    response = client.get("/home")
    assert response.status_code == 200, "Sanity test failed"
    assert 'name="template_id" value="1"' in response.text
    assert 'name="template_project" value="test-project"' in response.text
    assert 'name="template_region" value="nowhere"' in response.text
    assert 'name="service_account" value="sa@test.com"' in response.text


