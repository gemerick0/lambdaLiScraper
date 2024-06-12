const env=null;
let REACT_APP_BASE_URL = "https://app.kaspr.io";
let REACT_APP_API_URL = "https://api.kaspr.io/";
if (env == "preprod") {
  REACT_APP_BASE_URL = "https://staging.kaspr.io";
  REACT_APP_API_URL = "https://staging.api.kaspr.io/";
}
if (env == "local") {
  REACT_APP_BASE_URL = "http://localhost:3000";
  REACT_APP_API_URL = "http://localhost:8000/";
}
if (env == "prod") {
  REACT_APP_BASE_URL = "https://app.kaspr.io";
  REACT_APP_API_URL = "https://api.kaspr.io/";
}
const headers = {
  Accept: "application/json, text/plain, */*",
  "Content-Type": "application/json",
};
