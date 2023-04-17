from data import ans, csv_file
from flask import Flask, jsonify, render_template
from flask import send_file
app = Flask(__name__)


@app.route('/')
def home():
    return render_template('home.html')


@app.route("/most_affected_state")
def get_most_affected_state():
    return jsonify({'most_affected_state':ans[0]})



@app.route('/least_affected_state')
def get_least_affected_state():
    return jsonify({'least_affected_state': ans[1]})


@app.route('/highest_covid_cases')
def get_highest_covid_cases():
    return jsonify({'get_highest_covid_cases': ans[2]})


@app.route('/least_covid_cases')
def get_least_covid_cases():
    return jsonify({'get_least_covid_cases': ans[3]})


@app.route('/total_cases')
def get_total_cases():
    return jsonify({'Total Cases': ans[4]})


@app.route('/most_efficient_state')
def get_most_efficient_state():
    return jsonify({'most efficient_state':ans[5]})


@app.route('/least_efficient_state')
def get_least_efficient_state():
    return jsonify({'least efficient_state': ans[6]})


@app.route("/getcsv")
def getcsvfile():
    file_path = csv_file()
    return render_template('csv.html', file_path =file_path)


@app.route("/download-data-file")
def download():
    file_path = csv_file()
    return send_file(file_path, as_attachment=True)


if __name__ == '__main__':
    app.run(debug=True, port=5000)
