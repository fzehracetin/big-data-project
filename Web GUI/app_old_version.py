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

client = InsecureClient('http://localhost:9000')
file_name = ""
hdfs_input_path = "input"
hdfs_output_name = "outputTest"


@app.route('/')
def main():
    return render_template('main.html', run_display="block", result_display="none")


def download_from_hdfs(hdfs_path):
    global file_name
    client.download(hdfs_path, DOWNLOAD_FOLDER)
    file_name = hdfs_path.split('/')[-1]


def upload_to_hdfs(local_path, hdfs_path):
    client.upload(hdfs_path, local_path)
    # os.system('cmd /k "{}"'.format(cmd_command))
    # hdfs dfs - put. / input / * input


def run_job(hdfs_output_path, selected_function):  # "Average"
    cmd_command = "docker exec -it namenode hadoop jar MapReduce.jar " + selected_function + " input " + \
                  hdfs_output_path
    os.system('cmd /k "{}"'.format(cmd_command))
    return


@app.route('/', methods=['POST'])
def upload_file():
    start = time.time()
    uploaded_file = request.files['file']
    if uploaded_file.filename != '':
        uploaded_file.save(UPLOAD_FOLDER + "/" + secure_filename(uploaded_file.filename))

    selected_function = request.form.get("function")
    print("File: {}, Function: {}".format(uploaded_file.filename, selected_function))

    # upload file to hdfs
    upload_to_hdfs(UPLOAD_FOLDER + "/" + secure_filename(uploaded_file.filename), hdfs_input_path)

    # run jobs here
    run_job(hdfs_output_name, selected_function)

    end = time.time()
    time_elapsed = end - start
    return render_template('main.html', run_display="none", result_display="block", function=selected_function,
                           time="%.4f" % time_elapsed)


@app.route('/result', methods=['POST'])
def result():
    if request.form['Submit'] == 'Go Back':
        return render_template('main.html', run_display="block", result_display="none")
    elif request.form['Submit'] == 'Download the Result':
        download_from_hdfs(hdfs_output_name)
        return render_template('main.html', run_display="none", result_display="block")
        # return send_file(DOWNLOAD_FOLDER + "/" + file_name,
        #                  mimetype='text/plain',
        #                  attachment_filename=file_name,
        #                  as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True)

