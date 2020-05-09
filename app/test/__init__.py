import os
import tempfile

import pytest

from app import app
from app.db.init import init_db


@pytest.fixture
def client():
    db_fd, app.config['DATABASE'] = tempfile.mkstemp()
    app.config['TESTING'] = True

    with app.test_client() as client:
        with app.app_context():
            init_db()

        yield client

    os.close(db_fd)
    os.unlink(app.config['DATABASE'])
