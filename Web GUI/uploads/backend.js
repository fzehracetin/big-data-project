// CBOT API Testing Platform --backend
// Fatma Zehra Cetin 08.06.2021
const mongoose = require('mongoose')
const xlsx = require('node-xlsx')
const ExcelJS = require('exceljs')
const fs = require('fs')
const multer = require('multer');
const express = require("express");
var cors = require('cors');
var upload = multer({ dest: 'uploads/' })
const axios = require('axios');
const http = require('http')
const https = require('https')


mongoose.connect('mongodb+srv://admin:1234@flask-mongodb-cbot.1d7j4.mongodb.net/api-connection?retryWrites=true&w=majority', { useNewUrlParser: true })
    .then(() => console.log('Now connected to MongoDB!'))
    .catch(err => console.error('Something went wrong', err))

const Endpoint = mongoose.model('endpoints', new mongoose.Schema({
  id: String,
  url: String,
  method: String,
  headers: Object,
  request_key: String,
  request_body: Object,
  response_keys: Object,
  state: String,
  group_id: { type: mongoose.SchemaTypes.ObjectId, ref: 'groups'},
}))

const Group = mongoose.model('groups', new mongoose.Schema({
  group_name: String,
}))

const app = express();
app.use(cors());

app.use(express.json());

app.listen(3000, ()=> {console.log("server starting on port 3000");});

app.post("/testTheGroup", upload.single('thumbnail'), async (req, res, next) => {
  let group_name = req.body.group;
  let file_path = req.file.path;
  let data = readExcel(file_path, true);
  await testTheGroup(data, group_name, function(outputName, workbook) {
    console.log("POST: ", outputName);
    res.setHeader('Content-Type',  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', "attachment; filename=" + outputName);
    workbook.xlsx.write(res).then(() => {
      res.end();
      console.log("Response sent");
    });
  });
});

app.get("/listEndpoints", async (req, res) => {
  let result = await findEndpoints({});
  res.send(result);
});

app.get("/listGroups", async (req, res) => {
  let result = await findGroups({});
  res.send(result);
});

app.post("/updateEndpoint", async (req, res) => {
  console.log(req.body);
  const filter = {id: req.body.id};
  const update_query = req.body.update_query;

  if (update_query.group_name != undefined) {
    const group_id = await findGroupID(update_query.group_name);
    delete update_query.group_name;
    update_query.group_id = group_id;
    console.log(update_query)
  }
  let result = await updateEndpoint(filter, update_query);
});

app.post("/updateGroup", async (req, res) => {
  console.log(req.body);
  const update_query = req.body.update_query;
  const filter = {_id: mongoose.Types.ObjectId(req.body._id)};
  console.log(filter);
  let result = await updateGroup(filter, update_query);
});

app.post("/deleteEndpoint", async(req, res) => {
  console.log(req.body);
  const filter = req.body.filter;
  console.log(filter);
  let result = await deleteEndpoint(filter);
});

app.post("/deleteGroup", async(req, res) => {
  console.log(req.body);
  const filter = {_id: mongoose.Types.ObjectId(req.body._id)};
  console.log(filter);
  let result = await deleteGroup(filter);
});

app.post("/newEndpoint", async (req, res) => {
  console.log(req.body);
  const data = req.body.data;
  await createEndpoint(data.id,
                      data.url,
                      data.method,
                      data.headers,
                      data.request_key,
                      data.request_body,
                      data.response_keys,
                      data.state,
                      data.group_name);

});

app.post("/newGroup", async(req, res) => {
  const data = req.body.data;
  console.log(data)
  await createGroup(data.group_name);
});

async function findGroupID(group_name) {
    const group = await Group
        .findOne({'group_name': group_name});

    if (group == null) {
      console.log(group_name, "does not exist in groups collection.")
      return null
    }
    return group._id;
}

async function createEndpoint(id, url, method, headers, request_key, request_body, response_keys, state, group_name) {
  const group_id = await findGroupID(group_name)
  if (group_id == null){
    console.log("Group not found and called as null")
  }
  const endpoint = new Endpoint({
    id,
    url,
    method,
    headers,
    request_key,
    request_body,
    response_keys,
    state,
    group_id,
  })

  const result = await endpoint.save();
  console.log(result)
  return result;
}

async function createGroup(group_name) {
  const result = await findGroupID(group_name)
  if (result != null) {
    console.log(group_name, "already defined in collection!")
    return
  }
  const group = new Group ({
    group_name,
  })

  const new_group = await group.save();
  console.log(new_group)
}

async function findMatchedEndpoints(group_name) {
  const group_id = await findGroupID(group_name)
  if (group_id == null) {
    return
  }
  const endpoints = await Endpoint
        .find({"state": "active", "group_id": group_id})

  if (endpoints.length == 0) {
    console.log("No document found!!")
    return null
  }
  return endpoints
}

async function findEndpoints(query) {
  const endpoints = await Endpoint
        .find(query)
  if (endpoints.length == 0) {
    console.log("No document found!!")
    return null
  }
  else {
    console.log(endpoints)
  }
  return endpoints
}

async function findGroups(query) {
  const groups = await Group
        .find(query)
  if (groups.length == 0) {
    console.log("No document found!!")
    return null
  }
  return groups
}

async function deleteEndpoint(filter) {
  const result = await Endpoint.deleteOne(filter);
  console.log(
    `${result.matchedCount} document(s) matched the filter, deleted ${result.modifiedCount} document(s)`,
  );
  return result;
}

async function deleteGroup(filter) {
  const result = await Group.deleteOne(filter);
  console.log(
    `${result.matchedCount} document(s) matched the filter, deleted ${result.modifiedCount} document(s)`,
  );
  return result;
}

async function updateEndpoint(filter, updateElement) {
  const updateDoc = {$set: updateElement};
  const result = await Endpoint.updateOne(filter, updateDoc)
  console.log(
    `${result.matchedCount} document(s) matched the filter, updated ${result.modifiedCount} document(s)`,
  );
  return result;
}

async function updateGroup(filter, updateElement) {
  const updateDoc = {$set: updateElement};
  const result = await Group
        .updateOne(filter, updateDoc)
  console.log(
    `${result.matchedCount} document(s) matched the filter, updated ${result.modifiedCount} document(s)`,
  );
}

// async function sendRequest(endpoint, message, responses, indices, callback) {
//   const url = endpoint.url
//   let request_data = endpoint.request_body
//   request_data[endpoint.request_key] = message;
//   //console.log(endpoint.request_body, request_data)
//   axios ({
//     method: endpoint.method,
//     url: endpoint.url,
//     data: request_data,
//     headers: endpoint.headers,
//     httpAgent: new http.Agent({ keepAlive: true }),
//     httpsAgent: new https.Agent({ keepAlive: true }),
//   })
//   .then(function (response) {
//     let response_data = response.data;
//     console.log(response_data)
//     return_data = response_data
//     // response_keys = Object.keys(endpoint.response_keys)
//     // keys_lenght = response_keys.length
//     // return_data = {}
//     // for (let i=0; i< keys_lenght; i++) {
//     //   return_data[response_keys[i]] = response_data[response_keys[i]]
//     // }
//     // response = ""
//     return callback(return_data, responses, indices);
//   }).catch(function(error){
//     console.log(error)
//     return callback("", responses, indices);
//   });
//
// }

async function sendRequest(endpoint, message, responses, indices, callback) {
  const url = endpoint.url
  const protocol = url.split(":")[0]
  const ip = url.split("//")[1].split(":")[0]
  const port = url.split(":")[2].split("/")[0]
  let part_length = url.split("/")[2].length
  const path = url.split("//")[1].slice(part_length)

  const http = require(protocol)

  let request_data = endpoint.request_body
  request_data[endpoint.request_key] = message;
  //console.log("Request: ", request_data)
  const data = JSON.stringify(request_data)
  const options = {
    hostname: ip,
    port: port,
    path: path,
    method: endpoint.method,
    headers: endpoint.headers,
  }

  let response = ""
  const req = http.request(options, res => {
    console.log(`statusCode: ${res.statusCode}`)
    res.on('data', d => {
      response += d;
    })
    res.on('end', res => {
      let response_data = JSON.parse(response);
      console.log(response_data)
      // response_keys = Object.keys(endpoint.response_keys)
      // keys_lenght = response_keys.length
      // return_data = {}
      // for (let i=0; i< keys_lenght; i++) {
      //   return_data[response_keys[i]] = response_data[response_keys[i]]
      // }
      // response = ""
      return callback(response_data, responses, indices);
    })
  })

  req.on('error', error => {
    console.error(error)
    return callback("", responses, indices);
  })

  req.write(data)
  req.end()
}

function processResponseKeys(endpoints) {
  for (let i = 0; i < endpoints.length; i++) {
    endpoints[i].response_keys = endpoints[i].response_keys.split(',');
    for (let j = 0; j < endpoints[i].response_keys.length; j++) {
     endpoints[i].response_keys[j] = endpoints[i].response_keys[j].trim();
    }
  }
}

function writeToExcel(responses, test_data, endpoints, callback) {
  processResponseKeys(endpoints)
  const workbook = new ExcelJS.Workbook();
  const sheet = workbook.addWorksheet('Results')

  let columns = [{
    key: "message",
    header: "Message"
  }]

  let column_size = 0
  for (let i = 0; i < endpoints.length; i++) {
    let endpoint_keys = endpoints[i].response_keys;
    column_size += endpoint_keys.length;
    for(let j = 0; j < endpoint_keys.length; j++) {
      columns.push({
        key: "api:" + i.toString() + " key:" + j.toString(),
        header: endpoints[i].id + "_" + endpoint_keys[j]
      });
    }
  }

  sheet.columns = columns

  let data = []

  for (let i = 0; i < test_data.length; i++) {
    let row = {
      message: test_data[i]
    }
    for (let j = 0; j < endpoints.length; j++) {
        let object = responses.find(o => o.test_data === i && o.endpoint === j);
        let endpoint_keys = endpoints[j].response_keys;

        for (let k = 0; k < endpoint_keys.length; k++) {
          // console.log(responses)
          // console.log(typeof(endpoint_keys[k]))
          if (! endpoint_keys[k].includes('.')) {
            console.log(object.response[endpoint_keys[k]])
            if (typeof(object.response[endpoint_keys[k]]) === 'object') {
              try {
                row["api:" + j.toString() + " key:" + k.toString()] = object.response[endpoint_keys[k]][0];
              }
              catch {
                row["api:" + j.toString() + " key:" + k.toString()] = object.response[endpoint_keys[k]];
              }
            }
            else if (typeof(object.response[endpoint_keys[k]]) === 'string') {
              row["api:" + j.toString() + " key:" + k.toString()] = object.response[endpoint_keys[k]]
            }
          }
        }
    }
    data.push(row)
  }

  data.forEach((item, i) => {
    sheet.addRow(item);
  });
  let fileName = "output.xlsx";
  workbook.xlsx.writeFile(fileName).then(() => {
    console.log("Written to file--->", fileName);
    return callback(fileName, workbook);
  });
}

async function testTheGroup(test_data, group_name, callback) {
  const endpoints = await findMatchedEndpoints(group_name)

  let responses = []
  if (endpoints != null && endpoints != undefined) {
    const size = parseInt(endpoints.length) * parseInt(test_data.length)

    let my_promise = new Promise((resolve, reject) => {
      for (let i = 0; i < endpoints.length; i++) {
        for (let j = 0; j < test_data.length; j++) {
          current_indices = {endpoint: i,test_data: j}

          sendRequest(endpoints[i], test_data[j], responses, current_indices, function(data, responses, indices) {
            responses.push({
              endpoint: indices.endpoint,
              test_data: indices.test_data,
              response: data
            })

            if (responses.length == size)
              resolve()
          })
        }
      }
    });

    my_promise.then(() => { //if resolved
      console.log("All done!");
      writeToExcel(responses, test_data, endpoints, function(fileName, workbook) {
        console.log("test the group: ", fileName);
        return callback(fileName, workbook);
      });
    });
    my_promise.catch(() => { //rejected
      console.log("Rejected!!!")
    })
  }
}

function readExcel(path, header) {
  let file = xlsx.parse(path)
  let array = file[0].data
  let data = []
  let i = 0
  if (header == true)
    i = 1
  for (i; i < array.length; i++) {
    data.push(array[i][0])
  }
  return data
}

function loadTestData(path, header) {
  let splitted = path.split(".")
  const file_format = splitted[splitted.length - 1]
  let test_data = []

  if (file_format == "csv") {
    test_data = readCsv(path, header)
  } else if (file_format == "xlsx") {
    test_data = readExcel(path, header)
  }
  if (test_data.lenght > 0) {
    console.log("Test data loaded. Length: ", test_data.lenght)
  }
  return test_data
}

async function requestToEndpoint(endpoint_id, message) {
  let query = {
    "id": endpoint_id,
  }
  endpoint = await findEndpoints(query)
  let response;
  requestWithAxios(endpoint[0], message, [], {}, function(data, responses, indices) {
    //console.log(data)
  })
}
