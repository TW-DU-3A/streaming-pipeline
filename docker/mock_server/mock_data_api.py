from flask import Flask, Response, abort
import os
from pathlib import Path
import json
from flask import jsonify
from flask import Flask


BASE_DIR = Path(__file__).resolve(strict=True).parent
FILE_PATH = os.path.join(os.path.join(BASE_DIR, "resources"), "sf-mock-response.json")
OUTPUT_FILE_PATH = os.path.join(os.path.join(BASE_DIR, "resources"), "mock_stationMart_1.csv")

app = Flask(__name__)

@app.route('/mock-data')
def mock_data():
    with open(FILE_PATH) as file:
        data = json.load(file)
        return jsonify(data)

@app.route('/mock-response')
def mock_response():
    with open(OUTPUT_FILE_PATH) as file:
        contents = file.read()
        return Response(contents, content_type='text/plain; charset=utf-8')

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
