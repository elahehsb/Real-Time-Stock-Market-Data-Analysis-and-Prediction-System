from flask import Flask, render_template, jsonify
import subprocess
from influxdb import InfluxDBClient

app = Flask(__name__)
client = InfluxDBClient(host='localhost', port=8086)
client.switch_database('stock')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/stock')
def stock():
    results = client.query('SELECT * FROM stock')
    points = list(results.get_points())
    return jsonify(points)

@app.route('/api/predict')
def predict():
    result = subprocess.run(['spark-submit', 'machine_learning/stock_prediction_model.py'], capture_output=True, text=True)
    return jsonify({"result": result.stdout})

if __name__ == '__main__':
    app.run(debug=True)
