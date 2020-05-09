from flask import Flask

from .view import register_bp

app = Flask(__name__)

register_bp(app)

