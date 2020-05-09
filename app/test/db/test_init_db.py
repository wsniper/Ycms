import pytest

from app.test import client

from app.db.init import init_db
from app.db import execute
from app import app

def test_init_db(client):
    resp = client.get('/')
    assert b'404 Not Found' in resp.data
