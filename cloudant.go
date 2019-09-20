package cloudant

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	utils "github.com/alessiosavi/GoUtils"
	request "github.com/alessiosavi/Requests"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

/*
curl -k -X POST \
--header "Content-Type: application/x-www-form-urlencoded" \
--header "Accept: application/json" \
--data-urlencode "grant_type=urn:ibm:params:oauth:grant-type:apikey" \
--data-urlencode "apikey=*****************************" \
"https://iam.cloud.ibm.com/identity/token"

*/

// RetrieveToken is delegated to retrieve the token for authenticate the HTTP request. It can be used for 3600 seconds
// The method use the apikey related to your Cloudant instance for authenticate into the IBM Cloud, and return back the token
// that have to be used as Authorization token
// NOTE: Every request have to be sent using the token retrieved by this method as a 'Bearer Authorization"
func RetrieveToken(apikey string) string {
	headers := request.CreateHeaderList(`Accept`, `application/json`, `Content-Type`, `application/x-www-form-urlencoded`)
	encoded := url.Values{}
	encoded.Set("grant_type", "urn:ibm:params:oauth:grant-type:apikey")
	encoded.Set("apikey", apikey)
	url := "https://iam.cloud.ibm.com/identity/token"
	response := request.SendRequest(url, `POST`, headers, []byte(encoded.Encode()))
	value := gjson.Get(string(response.Body), "access_token")
	return value.String()
}

// PingCloudant is delegated to verify that the Cloudant DB instance can be reached
// token: bearer auth header retrieved from RetrieveToken()
// host: URL related to the DB instance
func PingCloudant(token, url string) bool {
	url += `/`
	headers := request.CreateHeaderList("Authorization", "Bearer "+token)
	fmt.Println(request.SendRequest(url, `GET`, headers, nil))
	return true
}

// RetrieveAllDB is delegated to return a list that contains the DB names related to the given DB instance host
// token: bearer auth header retrieved from RetrieveToken()
// host: URL related to the DB instance
func RetrieveAllDB(token, url string) []string {
	url += `/_all_dbs`
	headers := request.CreateHeaderList("Authorization", "Bearer "+token)
	response := request.SendRequest(url, `GET`, headers, nil)
	var dbList []string
	json.Unmarshal(response.Body, &dbList)

	fmt.Println("Database => ", dbList, ` | Len -> `, len(dbList))
	return dbList
}

// RetrieveDB is delegated to retrieve the information related to the given DB
// token: bearer auth header retrieved from RetrieveToken()
// url: URL related to the DB instance
// dbName: DB that we want to retrieve the information
func RetrieveDB(token, url, dbName string) string {
	url += `/` + dbName
	headers := request.CreateHeaderList("Authorization", "Bearer "+token)
	response := request.SendRequest(url, `GET`, headers, nil)
	var dbInfo string
	json.Unmarshal(response.Body, &dbInfo)
	fmt.Println("Database => ", dbInfo)
	return dbInfo
}

// RetrieveAllDocs is delegated to retrieve all documents associated to the given DB
// token: bearer auth header retrieved from RetrieveToken()
// url: URL related to the DB instance
// dbName: DB that we want to retrieve the information
func RetrieveAllDocs(token, url, dbName string) string {
	url += `/` + dbName + `/all_docs`
	headers := request.CreateHeaderList("Authorization", "Bearer "+token)
	response := request.SendRequest(url, `GET`, headers, nil)
	var docs string
	json.Unmarshal(response.Body, &docs)
	fmt.Println("Docs => ", docs)
	return docs
}

// CreateDatabase is delegated to initializate a new database.
// In first instance it will check if the DB alredy exist (not necessary, Cloudant will raise an exception)
// token: bearer auth header retrieved from RetrieveToken()
// url: URL related to the DB instance
// dbName: DB that we want to retrieve the information
// partitioned: boolean value for enabled partitioned option
func CreateDatabase(token, databaseName, url string, partitioned bool) bool {
	// Check if DB alredy exists
	dbList := RetrieveAllDB(token, url)
	if len(dbList) > 0 {
		for i := range dbList {
			if strings.Compare(dbList[i], databaseName) == 0 {
				zap.S().Warn("CreateDatabase | Database alredy exist!")
				return false
			}
		}
	}
	zap.S().Debug("CreateDatabase | Database [", databaseName, "] does exist! Creating new one ..")

	url += `/` + databaseName + `?partitioned=` + strconv.FormatBool(partitioned)
	headers := request.CreateHeaderList("Authorization", "Bearer "+token)
	fmt.Println(request.SendRequest(url, `PUT`, headers, nil))
	return true
}

// RemoveDB is delegated to delete the given DB
// token: bearer auth header retrieved from RetrieveToken()
// url: URL related to the DB instance
// dbName: DB that we want to retrieve the information
func RemoveDB(token, databaseName, url string) bool {
	url += `/` + databaseName
	headers := request.CreateHeaderList("Authorization", "Bearer "+token)
	fmt.Println(request.SendRequest(url, `DELETE`, headers, nil))
	return true
}

// ====== DOCUMENT API ======

// InsertJSON is delegated to insert a new document into the given DB
// token: bearer auth header retrieved from RetrieveToken()
// url: URL related to the DB instance
// databaseName: DB that we want to retrieve the information
// json: document to insert
func InsertJSON(token, url, databaseName, json string) bool {
	url += `/` + databaseName
	headers := request.CreateHeaderList("Authorization", "Bearer "+token, `Content-Type`, `application/json`)
	response := request.SendRequest(url, `POST`, headers, []byte(json))
	return response.StatusCode == 200
}

// InsertBulkJSON is delegated to insert a list of document. It will concatenate all the json in input and
// will insert all the document in a single request
// token: bearer auth header retrieved from RetrieveToken()
// url: URL related to the DB instance
// databaseName: DB that we want to retrieve the information
// jsons: list of document that we want to insert in bulk
func InsertBulkJSON(token, url, databaseName string, jsons []string) bool {
	url += `/` + databaseName + `/_bulk_docs`
	headers := request.CreateHeaderList("Authorization", "Bearer "+token, `Content-Type`, `application/json`)

	json := `{"docs":[`
	for i := range jsons {
		json = utils.Join(json, jsons[i], `,`)
	}
	json = strings.TrimSuffix(json, `,`)
	json += `]}`
	response := request.SendRequest(url, `POST`, headers, []byte(json))
	zap.S().Info("Resp-> ", string(response.Body), ` | Status: `, response.StatusCode)
	return response.StatusCode == 200
}
