from flask import Flask, Response, abort
import os
from pathlib import Path
import json
from flask import jsonify
from flask import Flask


BASE_DIR = Path(__file__).resolve(strict=True).parent.parent
FILE_PATH = os.path.join(os.path.join(BASE_DIR, "resources"), "sf-mock-response.json")
app = Flask(__name__)

@app.route('/mock-data')
def mock_data():
    with open(FILE_PATH) as file:
        data = json.load(file)
        return jsonify(data)
