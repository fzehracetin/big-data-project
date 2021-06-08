from flask import Flask, render_template, request, send_file
from werkzeug.utils import secure_filename
import os
import time
from uuid import uuid4

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
if not os.path.isdir(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)

DOWNLOAD_FOLDER = os.path.join(os.getcwd(), 'downloads')
if not os.path.isdir(DOWNLOAD_FOLDER):
    os.mkdir(DOWNLOAD_FOLDER)

app = Flask(__name__)

file_name = ""
hdfs_output_name = ""
name_node_data_path = "/home/datasets/"


@app.route('/')
def main():
    return render_template('main.html', run_display="block", result_display="none")


def download_from_hdfs():
    name_node_output_path = name_node_data_path + hdfs_output_name + ".txt"
    local_output_path = DOWNLOAD_FOLDER + "\\" + hdfs_output_name + ".txt"
    # copy output file to name node
    copy_to_name_node = "docker exec -it namenode hadoop fs -getmerge " + hdfs_output_name + " " + \
                        name_node_output_path
    # copy file to local file system
    copy_to_local_fs = "docker cp namenode:" + name_node_output_path + " " + local_output_path
    os.system('cmd /c "{} & {}"'.format(copy_to_name_node, copy_to_local_fs))
    return


def upload_to_hdfs(local_input_path, hdfs_input_path):
    name_node_input_path = name_node_data_path + file_name
    # upload file to name node
    upload_to_name_node = "docker cp " + local_input_path + " namenode:" + name_node_input_path
    # put file to hadoop file system
    put_to_hdfs = "docker exec -it namenode hadoop fs -put " + name_node_input_path + " " + hdfs_input_path

    os.system('cmd /c "{} & {}"'.format(upload_to_name_node, put_to_hdfs))
    return
    

def run_job(selected_function, hdfs_input_path):  # "Average"
    # run mapreduce job
    run_job_command = "docker exec -it namenode hadoop jar mapReduce.jar " + selected_function + " " + \
                      hdfs_input_path + " " + hdfs_output_name
    os.system('cmd /c "{}"'.format(run_job_command))
    return


@app.route('/', methods=['POST'])
def upload_file():
    global hdfs_output_name, file_name
    start = time.time()
    uploaded_file = request.files['file']
    if uploaded_file.filename != '':
        file_name = secure_filename(uploaded_file.filename)
        uploaded_file.save(UPLOAD_FOLDER + "/" + file_name)

    selected_function = request.form.get("function")
    print("File: {}, Function: {}".format(file_name, selected_function))

    hdfs_input_path = "input"
    # random name for output file
    hdfs_output_name = str(uuid4())
    print("Output name: ", hdfs_output_name)
    # upload file to hdfs
    upload_path = UPLOAD_FOLDER + "\\" + file_name
    upload_to_hdfs(upload_path, hdfs_input_path)
    # run jobs here
    run_job(selected_function, hdfs_input_path)
    # save mapreduce output to local file system
    download_from_hdfs()
    end = time.time()
    time_elapsed = end - start
    return render_template('main.html', run_display="none", result_display="block", function=selected_function,
                           time="%.2f" % time_elapsed)


@app.route('/result', methods=['POST'])
def result():
    if request.form['Submit'] == 'Go Back':
        return render_template('main.html', run_display="block", result_display="none")
    elif request.form['Submit'] == 'Download the Result':
        f_name = file_name.split(".")[0]
        return send_file(DOWNLOAD_FOLDER + "\\" + hdfs_output_name + ".txt", mimetype='text/plain',
                         attachment_filename=f_name + "_output.txt", as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True)
