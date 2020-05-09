from app.test import client
from app.conf import URL

def test_admin_index_url(client):
    resp = client.get(URL.admin or '/admin')
    print(URL.admin)
    print(resp.data)

def test_user_index_url(client):
    resp = client.get(URL.user or '/user')
    print(URL.user)
    print(resp.data)

def test_frontend_index_url(client):
    resp = client.get('/')
    print('frontend index')
    print(resp.data)
