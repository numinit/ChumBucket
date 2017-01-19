//code for sending generic requests to server
const url = 'http://hostname.com'

function isJSONResponse(r) {
  return r.headers.get('Content-Type').indexOf('json') > 0
}

const resource = (method, endpoint, payload) => {
  const options = {
    method,
    credentials: 'include',
    headers: {
      'Content-Type': 'application/json'
    }
  }

  if (payload) options.body = JSON.stringify(payload)

  return fetch(`${url}/${endpoint}`, options)
    .then(r => {
      console.log("Fetch response is ", r.headers.get('Content-Type'))
      if (r.status === 200) {
        if (isJSONResponse(r)){
          return r.json().then(json => json)
        }
        else {
          return r.text().then(text => text)
        }
      } else {
        console.log(`${method} ${endpoint} ${r.statusText}`)
        if (isJSONResponse(r)) {
          return r.json().then(
            json => {throw new Error(json.error)}
          )
        }
      }
    })
}

/////////////////////
//code implementing endpoint requests
/////////////////////

const dummyURI = "dummyURI"
const dummyName = "dummyName"

const sendQueryToServer = (code) =>
{
  return resource('POST', 'analysis/submit', { dummyURI,dummyName,code })
}

const getJobStatusFromServer = (jobURI) =>
{
  return resource('GET', `analysis/status?uri=${jobURI}`)
}

const getJobResultsFromServer = (jobURI) =>
{
  return resource('GET', `analysis/results?uri=${jobURI}`)
}

/////////////////////////////
//code to interact with html
//////////////////////////////


const sendQuery = () =>
{
  var iFrequency = 2000; // frequency of check
  var myInterval;
  var job = {status: "IDLE"}

  query = document.getElementById("query").innerHTML
  sendQueryToServer(query).then(jobJson =>
  {
    job.status = jobJson.status
    job.uri = jobJson.uri
    if(job.status === "SUCCEEDED")
    {
      clearInterval(myInterval);  // stop checking server

      document.getElementById("resultsButton").style.display = "inline"
      displayResults(job.uri)
    }
    setInterval("getJobStatus()", iFrequency, job);
  })
}

const getJobStatus = (job) =>
{
  var jsonToReturn = {}
  getJobStatusFromServer(job.uri).then(jobJson =>
  {
    jsonToReturn.uri = jobJson.uri;
    jsonToReturn.status = jobJson.status
    return jsonToReturn
  })
  return jsonToReturn
}

const displayResults = (jobURI) =>
{
  url = ""
  getJobResultsFromServer(jobURI).then(resultsJson =>
  {
    window.location.href = resultsJson.url
  })
}
