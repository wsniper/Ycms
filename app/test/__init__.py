import os
import tempfile

import pytest

from app import create_app
from app.db.init import init_db

from app.conf import DB


@pytest.fixture
def client():
    app_ins = create_app()
    db_fd, db_name = tempfile.mkstemp()
    app_ins.config['TESTING'] = True

    driver = DB.driver
    DB.driver = 'sqlite:///' + db_name
    with app_ins.test_client() as client:
        with app_ins.app_context():
            init_db()

        yield client

    os.close(db_fd)
    os.unlink(db_name)
    DB.driver = driver


@pytest.fixture
def app_with_db_inited():
    app_ins = create_app()
    db_fd, db_name = tempfile.mkstemp()
    app_ins.config['TESTING'] = True

    driver = DB.driver
    DB.driver = 'sqlite:///' + db_name
    with app_ins.app_context():
        init_db()

    yield app_ins

    os.close(db_fd)
    os.unlink(db_name)
    DB.driver = driver


