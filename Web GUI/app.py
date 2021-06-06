from flask import Flask, render_template, request, send_file
from werkzeug.utils import secure_filename
from hdfs import InsecureClient
import os
import time

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
if not os.path.isdir(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)

DOWNLOAD_FOLDER = os.path.join(os.getcwd(), 'downloads')
if not os.path.isdir(DOWNLOAD_FOLDER):
    os.mkdir(DOWNLOAD_FOLDER)

app = Flask(__name__)

file_name = "son.txt"


@app.route('/')
def main():
    return render_template('main.html', run_display="block", result_display="none")


def download_from_hdfs(hdfs_path):
    global file_name
    client = InsecureClient('http://host:port')
    client.download(hdfs_path, DOWNLOAD_FOLDER)
    file_name = hdfs_path.split('/')[-1]


def upload_to_hdfs(local_path, hdfs_path):
    client = InsecureClient('http://host:port')
    client.upload(hdfs_path, local_path)


def run_job(hdfs_path, function):
    return


@app.route('/', methods=['POST'])
def upload_file():
    start = time.time()
    uploaded_file = request.files['file']
    # if uploaded_file.filename != '':
    #     uploaded_file.save(UPLOAD_FOLDER + "/" + secure_filename(uploaded_file.filename))

    selected_function = request.form.get("function")
    print("File: {}, Function: {}".format(uploaded_file.filename, selected_function))

    # upload file to hdfs
    hdfs_path = ""
    # upload_to_hdfs(UPLOAD_FOLDER + "/" + secure_filename(uploaded_file.filename), hdfs_path)

    # run jobs here
    run_job(hdfs_path, selected_function)

    end = time.time()
    time_elapsed = end - start
    return render_template('main.html', run_display="none", result_display="block", function=selected_function,
                           time="%.4f" % time_elapsed)


@app.route('/result', methods=['POST'])
def result():
    if request.form['Submit'] == 'Go Back':
        return render_template('main.html', run_display="block", result_display="none")
    elif request.form['Submit'] == 'Download the Result':
        return send_file(DOWNLOAD_FOLDER + "/" + file_name,
                         mimetype='text/plain',
                         attachment_filename=file_name,
                         as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True)

