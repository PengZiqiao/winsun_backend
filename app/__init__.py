from flask import Flask
from flask_restful import Api

from app.ext import db
from app.restful_api import HelloWorld
from config import Config


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # datebase
    db.init_app(app)

    # restful
    api = Api(app)
    api.add_resource(HelloWorld, '/')

    return app


app = create_app()
