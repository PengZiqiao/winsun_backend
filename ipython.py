from app import create_app
from app.model import *
from app.ext import *

app = create_app()
app.app_context().push()
