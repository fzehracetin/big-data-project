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

client = InsecureClient('localhost:9870')
file_name = ""
hdfs_input_path = "input"
hdfs_output_path = "outputTestWeb5"


@app.route('/')
def main():
    return render_template('main.html', run_display="block", result_display="none")


def download_from_hdfs(hdfs_path):
    global file_name
    client.download(hdfs_path, DOWNLOAD_FOLDER)
    file_name = hdfs_path.split('/')[-1]


def upload_to_hdfs(local_path, hdfs_path, selected_function):
    filename = local_path.split("\\")[-1]
    print(filename)
    cmd_command = "docker cp " + local_path + " namenode:/home/datasets/" + filename
    print(cmd_command)
    
    cmd_command2 = "docker exec -it namenode hadoop fs -put /home/datasets/" + filename + " input"
    print(cmd_command2)
    
    #cmd_command3 = "docker exec -it namenode hdfs dfs -put ./input/* input"
    cmd_command3 = "docker exec -it namenode hadoop jar mapReduce.jar " + selected_function + " input " + \
                  hdfs_output_path
    
    cmd_command4 = "docker exec -it namenode hadoop fs -getmerge " + hdfs_output_path + " home/datasets/" + hdfs_output_path + ".txt"
    cmd_command5 = "docker cp namenode:home/datasets/" + hdfs_output_path + ".txt downloads\\" + hdfs_output_path + ".txt"
    
    os.system('cmd /c "{} & {} & {} & {} & {}"'.format(cmd_command, cmd_command2, cmd_command3, cmd_command4, cmd_command5))
    

def run_job(hdfs_output_path, selected_function):  # "Average"
    cmd_command = "docker exec -it namenode hadoop jar mapReduce.jar " + selected_function + " input " + \
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
    full_path = "C:\\Users\\fazb7\\Desktop\\SchoolProjects\\BigData\\Web_GUI\\WebGUI\\uploads\\" + uploaded_file.filename
    print(full_path)
    upload_to_hdfs(full_path, hdfs_input_path, selected_function)
    #UPLOAD_FOLDER + "\\" + secure_filename(uploaded_file.filename),
    # run jobs here
    #run_job(hdfs_output_path, selected_function)
    
    end = time.time()
    time_elapsed = end - start
    return render_template('main.html', run_display="none", result_display="block", function=selected_function,
                           time="%.4f" % time_elapsed)


@app.route('/result', methods=['POST'])
def result():
    if request.form['Submit'] == 'Go Back':
        return render_template('main.html', run_display="block", result_display="none")
    elif request.form['Submit'] == 'Download the Result':
        #download_from_hdfs(hdfs_output_path)
        #return render_template('main.html', run_display="none", result_display="block")
        return send_file(DOWNLOAD_FOLDER + "\\" + hdfs_output_path + ".txt", mimetype='text/plain', attachment_filename=file_name, as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True)

