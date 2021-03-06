package cloudant

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	utils "github.com/alessiosavi/GoUtils"
	request "github.com/alessiosavi/Requests"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

// Conf struct is delegated to save the information related to the Cloudant account
type Conf struct {
	// Key used for retrieve the bearer token
	Apikey string `json:"apikey"`
	// Hostname of the cloudant instance
	Host                 string `json:"host"`
	IAMApikeyDescription string `json:"iam_apikey_description"`
	IAMApikeyName        string `json:"iam_apikey_name"`
	IAMRoleCrn           string `json:"iam_role_crn"`
	IAMServiceidCrn      string `json:"iam_serviceid_crn"`
	// Password for authenticate to the service
	Password string `json:"password"`
	// Port for reach the server
	Port int `json:"port"`
	// Url using BasicAuth, avoid using this url with basicauth for performance
	URL string `json:"url"`
	// Username related to the Cloudant instance
	Username string `json:"username"`
}

// Auth struct is delegated to store the necessary token for authenticate to the service
// Cloudant HTTP call can be made using one of authentication credential
type Auth struct {
	// USER:PASSWORD encoded for basic auth (basic auth headers)
	BasicAuth string
	// Cookie for authenticate the session (cookie)
	SessionCookie string
	// IAM Token related to IBM Cloud service (bearer auth headers)
	IAMToken string
	// URL related to the Cloudant instance DB
	DBUrl string
}

// InitAuth is delegated to initialize the Authentication details for authenticate every request.
// The method will initialize the three method for authenticate the HTTP request:
// - BasicAuth -> Create the header for authenticate the request
// - SessionCookie -> Initialize a new session cookie-based and return the cookie for authenticate the request
// - IAMToken -> Retrieve the IAM token that expire after 3600 seconds
func (conf Conf) InitAuth() Auth {
	var auth Auth

	if conf.Host == "" {
		zap.S().Error("InitAuth | Host not provided!")
		return auth
	}

	auth.DBUrl = strings.TrimSpace(`https://` + conf.Host)
	if conf.Apikey == "" || conf.Username == "" || conf.Password == "" {
		zap.S().Error("InitAuth | Unable to retreieve data from configuration -> ", conf)
		return auth
	}
	zap.S().Debug("InitAuth | Initializing authentication token")
	rawHeaders := conf.Username + `:` + conf.Password
	basicAuth := `Authorization: Basic ` + base64.StdEncoding.EncodeToString([]byte(rawHeaders))
	zap.S().Debug("InitAuth | BasicAuth headers ->  ", basicAuth)
	auth.BasicAuth = strings.TrimSpace(basicAuth)

	zap.S().Debug("InitAuth | Initializing session cookie based")
	auth.SessionCookie = conf.GenerateCookie(auth.DBUrl)
	zap.S().Debug("InitAuth | Initializing IAM Token")
	auth.IAMToken = strings.TrimSpace(conf.GenerateIBMToken())
	zap.S().Debug("InitAuth | Auth struct configured! -> ", auth)
	return auth
}

// GetSessionInfo is delegated to retrieve the information related to the current session
func (auth Auth) GetSessionInfo() string {
	zap.S().Debug("GetSessionInfo | START | Retrieving information related to the current session")
	if strings.TrimSpace(auth.SessionCookie) == "" {
		zap.S().Error("GetSessionInfo | Cookie not initialized")
		return ""
	}

	headers := request.CreateHeaderList(`Accept`, `application/json`, `Set-Cookie`, auth.SessionCookie)
	URL := auth.DBUrl + `/_session`
	resp := request.SendRequest(URL, `GET`, headers, nil)
	zap.S().Debug("GetSessionInfo | HTTP Code: ", resp.StatusCode, " | Body: ", string(resp.Body))
	if resp.StatusCode != 200 {
		zap.S().Error("GetSessionInfo | ERROR! Something went wrong ... | Body: [", string(resp.Body), "]")
		return ""
	}
	return string(resp.Body)
}

// =================== AUTHENTICATION METHOD ===================

// GenerateIBMToken is delegated to retrieve the token for authenticate the HTTP request. It can be used for 3600 seconds
// https://cloud.ibm.com/docs/iam?topic=iam-iamtoken_from_apikey
// The method use the apikey related to your Cloudant instance for authenticate into the IBM Cloud, and return back the token
// that have to be used as Authorization token
// NOTE: Every request have to be sent using the token retrieved by this method as a 'Bearer Authorization"
func (conf Conf) GenerateIBMToken() string {
	zap.S().Debug("GenerateIBMToken | START | Asking for a new token for APIKEY [", conf.Apikey, "] ...")

	if strings.TrimSpace(conf.Apikey) == "" {
		zap.S().Error("GenerateIBMToken | Empty apikey")
		return ""
	}
	headers := request.CreateHeaderList(`Accept`, `application/json`, `Content-Type`, `application/x-www-form-urlencoded`)
	encoded := url.Values{}
	encoded.Set("grant_type", "urn:ibm:params:oauth:grant-type:apikey")
	encoded.Set("apikey", conf.Apikey)
	url := "https://iam.cloud.ibm.com/identity/token"
	zap.S().Debug("GenerateIBMToken | Sending request to URL: [", url, "]")
	resp := request.SendRequest(url, `POST`, headers, []byte(encoded.Encode()))
	zap.S().Debug("GenerateIBMToken | HTTP Code: ", resp.StatusCode, " | Body: ", string(resp.Body))
	if resp.StatusCode != 200 {
		zap.S().Error("GenerateIBMToken | ERROR! Something went wrong ... | Body: [", string(resp.Body), "]")
		return ""
	}
	value := gjson.Get(string(resp.Body), "access_token")
	return value.String()
}

// GenerateCookie is delegated to inititialize a new session cookie based
// https://cloud.ibm.com/docs/services/Cloudant?topic=cloudant-authentication#cookie-authentication
// The method use the username and password for initialize a new Cloudant session for authenticate into IBM Cloud Cloudant instance
// that have to be used as Authorization token
// NOTE: Every request have to be sent using the token retrieved by this method as a 'Bearer Authorization"
func (conf Conf) GenerateCookie(URL string) string {
	zap.S().Debug("GenerateCookie | START | Asking for a new token for SESSION COOKIE [", conf.Username, ":", conf.Password, "] ...")

	if strings.TrimSpace(conf.Username) == "" || strings.TrimSpace(conf.Password) == "" {
		zap.S().Error("GenerateCookie | Empty user or pass")
		return ""
	}
	headers := request.CreateHeaderList(`Accept`, `application/json`, `Content-Type`, `application/x-www-form-urlencoded`)

	URL += `/_session`
	zap.S().Debug("GenerateCookie | Sending request to URL: [", URL, "] with body: [", `name=`+conf.Username+`&password=`+conf.Password, "]")
	resp := request.SendRequest(URL, `POST`, headers, []byte(`name=`+conf.Username+`&password=`+conf.Password))
	zap.S().Debug("GenerateCookie | HTTP Code: ", resp.StatusCode, " | Body: ", string(resp.Body))
	if resp.StatusCode != 200 {
		zap.S().Error("GenerateCookie | ERROR! Something went wrong ... | Body: [", string(resp.Body), "]")
		return ""
	}
	zap.S().Debug("GenerateCookie | Headers ->", resp.Headers)
	// Save the response cookie into a map
	var cookies map[string]string
	cookies = make(map[string]string)
	for i := range resp.Headers {
		data := resp.Headers[i]
		zap.S().Debug("GenerateCookie | Analyzing -> ", data)
		// Filter only the "Set-Cookie" headers
		if strings.Contains(data, "Set-Cookie") {
			// Extracting everything after "Set-Cookie:" until the end of the string
			raw := data[len("Set-Cookie:"):len(data)]
			zap.S().Debug("GenerateCookie | Extracting cookie from data | Raw: ", raw)
			// Understand where is the '=' that split key and value
			splitIndex := strings.Index(raw, "=")
			// Extracting everything starting after "Set-Cookie:" until the end of first '='
			key := strings.TrimSpace(raw[0:splitIndex])
			// Extracting everything after the first "=" (+1) until the end of the string
			value := `"` + strings.TrimSpace(raw[splitIndex+1:len(raw)])
			value = strings.Replace(value, ";", `";`, 1)
			zap.S().Debug("GenerateCookie | Key: ", key)
			zap.S().Debug("GenerateCookie | Value: ", value)
			cookies[key] = value
		}
	}
	zap.S().Debug("GenerateCookie | Found ", len(cookies), " cookies -> [", cookies, "]")
	if len(cookies) == 0 {
		zap.S().Error("GenerateCookie | Unable to retrieve cookie")
		return ""
	}
	var value string
	value = ""
	for key := range cookies {
		if key == "AuthSession" {
			zap.S().Debug("TestGenerateCookie | Auth cookie found!")
			zap.S().Info("TestGenerateCookie | Key: ", key, " | Value: ", cookies[key])
			value = cookies[key]
		}
	}

	return `AuthSession=` + value
}

// PingCloudant is delegated to verify that the Cloudant DB instance can be reached
// token: bearer auth header retrieved from RetrieveToken()
// host: URL related to the DB instance
func (auth Auth) PingCloudant() bool {
	auth.DBUrl += `/`
	headers := request.CreateHeaderList(`Accept`, `application/json`, "Authorization", "Bearer "+auth.IAMToken)
	fmt.Println(request.SendRequest(auth.DBUrl, `GET`, headers, nil))
	return true
}

// =================== DATABASE METHOD ===================
// https://cloud.ibm.com/docs/services/Cloudant?topic=cloudant-databases

// CreateDB is delegated to initializate a new database.
// https://cloud.ibm.com/docs/services/Cloudant?topic=cloudant-databases#create-database
// token: bearer auth header retrieved from RetrieveToken()
// url: URL related to the DB instance
// dbName: DB that we want to retrieve the information
// partitioned: boolean value for enabled partitioned option
func (auth Auth) CreateDB(dbName string, partitioned bool) bool {
	// Check if DB alredy exists
	zap.S().Debug("CreateDB | START | Creating a new DB [", dbName, "] ...")

	if auth.IAMToken == "" {
		zap.S().Debug("CreateDB | IAM token not provided :/")
		return false
	}

	if dbName == "" {
		zap.S().Debug("CreateDB | DB name not provided :/")
		return false
	}

	url := auth.DBUrl + `/` + dbName + `?partitioned=` + strconv.FormatBool(partitioned)
	headers := request.CreateHeaderList(`Accept`, `application/json`, "Authorization", "Bearer "+auth.IAMToken)
	zap.S().Debug("CreateDB | Sending request to URL: [", url, "]")
	resp := request.SendRequest(url, `PUT`, headers, nil)
	zap.S().Debug("CreateDB | Request executed -> Data: [", string(resp.Body), "] | Status: [", resp.StatusCode, "]")
	if resp.StatusCode == 201 || resp.StatusCode == 202 {
		zap.S().Debug("CreateDB | DB ", dbName, " created succesully!")
	} else if resp.StatusCode == 400 {
		zap.S().Error("CreateDB | DB ", dbName, " have an invalid name, DB not created!!")
		return false
	} else if resp.StatusCode == 412 {
		zap.S().Error("CreateDB | DB ", dbName, " alredy exist!!!")
		return false
	}
	return true
}

// GetDBDetails is delegated to retrieve the information related to the given DB
// https://cloud.ibm.com/docs/services/Cloudant?topic=cloudant-databases#getting-database-details
// token: bearer auth header retrieved from RetrieveToken()
// url: URL related to the DB instance
// dbName: DB that we want to retrieve the information
func (auth Auth) GetDBDetails(dbName string) string {
	zap.S().Debug("GetDBDetails | START | Retrieving information related to DB [", dbName, "] ...")
	if dbName == "" {
		zap.S().Debug("GetDBDetails | DBName not provided!")
		return ""
	}
	URL := auth.DBUrl + `/` + dbName
	headers := request.CreateHeaderList(`Accept`, `application/json`, `Set-Cookie`, auth.SessionCookie)
	zap.S().Debug("GetDBDetails | Sending request to URL: [", URL, "]")
	resp := request.SendRequest(URL, `GET`, headers, nil)
	zap.S().Debug("GetDBDetails | HTTP Code: ", resp.StatusCode, " | Body: ", string(resp.Body))
	if resp.StatusCode != 200 {
		zap.S().Error("GetDBDetails | Unable to fetch response :/")
		return ""
	}
	zap.S().Debug("GetDBDetails | Request executed -> " + string(resp.Body))
	var dbInfo string
	json.Unmarshal(resp.Body, &dbInfo)
	zap.S().Debug("GetDBDetails | DB retrieved => ", dbInfo)
	return dbInfo
}

// GetAllDBs is delegated to fetch and retrieve all DB(s) name from the Cloudant instance
// https://cloud.ibm.com/docs/services/Cloudant?topic=cloudant-databases#get-a-list-of-all-databases-in-the-account
// token: bearer auth header retrieved from RetrieveToken()
// host: URL related to the DB instance
func (auth Auth) GetAllDBs(url string) []string {
	zap.S().Debug("GetAllDBs | START | Retrieving information related to all DBs ...")
	URL := url + `/_all_dbs`
	headers := request.CreateHeaderList(`Accept`, `application/json`, `Set-Cookie`, auth.SessionCookie)
	zap.S().Debug("GetAllDBs | Sending request to URL: [", URL, "]")
	resp := request.SendRequest(URL, `GET`, headers, nil)
	zap.S().Debug("GetAllDBs | HTTP Code: ", resp.StatusCode, " | Body: ", string(resp.Body))
	if resp.StatusCode != 200 {
		zap.S().Error("GetAllDBs | Unable to fetch response :/")
		zap.S().Error("GetAllDBs | HTTP Code: ", resp.StatusCode, " | Body: ", string(resp.Body))
		return nil
	}
	var dbList []string
	json.Unmarshal(resp.Body, &dbList)
	fmt.Println("Database => ", dbList, ` | Len -> `, len(dbList))
	return dbList
}

// GetAllDocuments is delegated to retrieve all documents associated to the given DB
// https://cloud.ibm.com/docs/services/Cloudant?topic=cloudant-databases#get-documents
// token: bearer auth header retrieved from RetrieveToken()
// url: URL related to the DB instance
// dbName: DB that we want to retrieve the information
func (auth Auth) GetAllDocuments(dbName, additionalQuery string) string {
	zap.S().Debug("GetAllDocuments | START | Retrieving all documents from DB [", dbName, "] ...")
	URL := auth.DBUrl + `/` + dbName + `/_all_docs?include_docs=true` + additionalQuery
	headers := request.CreateHeaderList(`Accept`, `application/json`, `Set-Cookie`, auth.SessionCookie)
	zap.S().Debug("GetAllDocuments | Sending request to URL: [", URL, "]")
	resp := request.SendRequest(URL, `GET`, headers, nil)
	zap.S().Debug("GetAllDocuments | HTTP Code: ", resp.StatusCode, " | Body: ", string(resp.Body))
	var docs string
	json.Unmarshal(resp.Body, &docs)
	fmt.Println("Docs => ", docs)
	return docs
}

// RemoveDB is delegated to delete the given DB
// https://cloud.ibm.com/docs/services/Cloudant?topic=cloudant-databases#deleting-a-database
// token: bearer auth header retrieved from RetrieveToken()
// url: URL related to the DB instance
// dbName: DB that we want to retrieve the information
func (auth Auth) RemoveDB(dbName string) bool {
	zap.S().Debug("RemoveDB | Removing DB [", dbName, "]")
	url := auth.DBUrl + "/" + dbName
	headers := request.CreateHeaderList(`Accept`, `application/json`, "Authorization", "Bearer "+auth.IAMToken)
	resp := request.SendRequest(url, `DELETE`, headers, nil)
	zap.S().Debug("RemoveDB | HTTP Code: ", resp.StatusCode, " | Body: ", string(resp.Body))
	if resp.StatusCode == 200 || resp.StatusCode == 202 {
		zap.S().Debug("RemoveDB | DB ", dbName, " deleted succesully!")
	} else if resp.StatusCode == 404 {
		zap.S().Error("RemoveDB | DB ", dbName, " does not exist!")
		return false
	}
	return true
}

// ====== DOCUMENT API ======

// InsertDocument is delegated to insert a new document into the given DB
// https://cloud.ibm.com/docs/services/Cloudant?topic=cloudant-documents#create-document
// token: bearer auth header retrieved from RetrieveToken()
// url: URL related to the DB instance
// databaseName: DB that we want to retrieve the information
// json: document to insert
func InsertDocument(token, url, databaseName string, json []byte) bool {
	zap.S().Debug("InsertDocument | Inserting new document into DB [", databaseName, "]")
	if binary.Size(json) >= 1048576 {
		zap.S().Error("InsertDocument | 1MB Json limit exceed!")
		return false
	}
	url += `/` + databaseName
	headers := request.CreateHeaderList("Authorization", "Bearer "+token, `Content-Type`, `application/json`)
	zap.S().Debug("InsertDocument | Sending request to URL: [", url, "]")
	response := request.SendRequest(url, `POST`, headers, json)
	zap.S().Debug("InsertDocument | Request executed -> Data: [", response.Body, "] | Status: [", response.StatusCode, "]")
	return response.StatusCode == 200 || response.StatusCode == 202
}

// GetDocument is delegated to retrieve a specific document by the related mandatory `_id`
// https://cloud.ibm.com/docs/services/Cloudant?topic=cloudant-documents#read-document
// token: bearer auth header retrieved from RetrieveToken()
// url: URL related to the DB instance
// databaseName: DB that we want to retrieve the information
// _id: Key for retrieve the document
func GetDocument(token, url, databaseName, _id string) string {
	zap.S().Debug("GetDocument | Retrieving document from DB [", databaseName, "] with '_id': [", _id, "]")
	url += `/` + databaseName + `/` + _id
	headers := request.CreateHeaderList("Authorization", "Bearer "+token, `Content-Type`, `application/json`)
	zap.S().Debug("GetDocument | Sending request to URL: [", url, "]")
	response := request.SendRequest(url, `GET`, headers, nil)
	zap.S().Debug("GetDocument | Request executed -> Data: [", response.Body, "] | Status: [", response.StatusCode, "]")
	if response.StatusCode != 200 {
		zap.S().Debug("GetDocument | ERROR! Response code is not 200! [", response.StatusCode, "]")
		return ""
	}
	return string(response.Body)
}

// UpdateDocument is delegated to update a specific document by the related mandatory '_id' parameter
// https://cloud.ibm.com/docs/services/Cloudant?topic=cloudant-documents#update
// token: bearer auth header retrieved from RetrieveToken()
// url: URL related to the DB instance
// databaseName: DB that we want to retrieve the information
// _id: Key for retrieve the document
func UpdateDocument(token, url, databaseName, _id string) string {
	zap.S().Debug("UpdateDocument | Updating document from DB [", databaseName, "] with '_id': [", _id, "]")
	url += `/` + databaseName + `/` + _id
	headers := request.CreateHeaderList("Authorization", "Bearer "+token, `Content-Type`, `application/json`)
	zap.S().Debug("UpdateDocument | Sending request to URL: [", url, "]")
	response := request.SendRequest(url, `PUT`, headers, nil)
	zap.S().Debug("UpdateDocument | Request executed -> Data: [", response.Body, "] | Status: [", response.StatusCode, "]")
	if response.StatusCode == 202 {
		zap.S().Warn("UpdateDocument | WARNING! Update does not meet the quorum")
	} else if response.StatusCode == 409 {
		zap.S().Error("UpdateDocumet | ERROR! You have not provided the most recent '_rev' parameter")
		return ""
	} else if response.StatusCode == 200 {
		zap.S().Debug("UpdateDocument | Docyment updated!")
	}
	return string(response.Body)
}

// DeleteDocument is delegated to retrieve a specific document by the related `_id`
// https://cloud.ibm.com/docs/services/Cloudant?topic=cloudant-documents#delete-a-document
// token: bearer auth header retrieved from RetrieveToken()
// url: URL related to the DB instance
// databaseName: DB that we want to retrieve the information
// _id: Key for retrieve the document
func DeleteDocument(token, url, databaseName, _id, _rev string) string {
	zap.S().Debug("DeleteDocument | Deleting document from DB [", databaseName, "] with '_id': [", _id, "] and '_rev': [", _rev, "]")
	url += `/` + databaseName + `/` + _id + `?rev=` + _rev
	headers := request.CreateHeaderList("Authorization", "Bearer "+token, `Content-Type`, `application/json`)
	zap.S().Debug("DeleteDocument | Sending request to URL: [", url, "]")
	response := request.SendRequest(url, `DELETE`, headers, nil)
	zap.S().Debug("DeleteDocument | Request executed -> Data: [", response.Body, "] | Status: [", response.StatusCode, "]")
	if response.StatusCode == 202 {
		zap.S().Warn("DeleteDocument | WARNING! Update does not meet the quorum")
	} else if response.StatusCode == 409 {
		zap.S().Error("DeleteDocument | ERROR! You have not provided the most recent '_rev' parameter")
		return ""
	} else if response.StatusCode == 200 {
		zap.S().Debug("DeleteDocument | Docyment updated!")
	}
	return string(response.Body)
}

// InsertBulkDocument is delegated to insert a list of document. It will concatenate all the json in input and
// will insert all the document in a single request
// token: bearer auth header retrieved from RetrieveToken()
// url: URL related to the DB instance
// dbName: DB that we want to retrieve the information
// documents: list of document that we want to insert in bulk
func InsertBulkDocument(token, url, dbName string, documents []string) string {
	zap.S().Debug("InsertBulkDocument | Inserting ", len(documents), " in bulk into [", dbName, "] ...")
	url += `/` + dbName + `/_bulk_docs`
	headers := request.CreateHeaderList("Authorization", "Bearer "+token, `Content-Type`, `application/json`)
	json := `{"docs":[`
	for i := range documents {
		json = utils.Join(json, documents[i], `,`)
	}
	json = strings.TrimSuffix(json, `,`)
	json += `]}`
	zap.S().Debug("InsertBulkDocument | Sending request to URL: [", url, "]")
	response := request.SendRequest(url, `POST`, headers, []byte(json))
	zap.S().Debug("InsertBulkDocument | Request executed -> Data: [", response.Body, "] | Status: [", response.StatusCode, "]")
	if response.StatusCode == 202 {
		zap.S().Warn("InsertBulkDocument | WARNING! Update does not meet the quorum")
	} else if response.StatusCode == 201 {
		zap.S().Error(`InsertBulkDocument | ERROR! The request did succeed, but this success does not imply all documents were updated.
		Inspect the response body to determine the status of each requested change`)
	} else if response.StatusCode == 200 {
		zap.S().Debug("InsertBulkDocument | Documents inserted!")
	}
	return string(response.Body)
}

// ================= UTILS ==================
func initConf() Conf {
	file := utils.ReadFileContentC("conf.json")
	var conf Conf
	err := json.Unmarshal([]byte(file), &conf)
	if err != nil {
		fmt.Println("ERROR! File not found ", err)
		os.Exit(0)
	}
	return conf
}
