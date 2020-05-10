import os
import tempfile

import pytest

from app import create_app
from app.db.init import init_db


@pytest.fixture
def client():
    app_ins = create_app()
    db_fd, app_ins.config['DATABASE'] = tempfile.mkstemp()
    app_ins.config['TESTING'] = True

    with app_ins.test_client() as client:
        with app_ins.app_context():
            init_db()

        yield client

    os.close(db_fd)
    os.unlink(app_ins.config['DATABASE'])
