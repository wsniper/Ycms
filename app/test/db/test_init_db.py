import pytest

from app.test import client

from app.db.init import init_db
from app.db import execute

def test_init_db(client):
    resp = client.get('/')
