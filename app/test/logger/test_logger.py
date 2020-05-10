from app.test import client


def test_log_warning(client):
    client.get('/user')


