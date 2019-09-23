package cloudant

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"go.uber.org/zap"
	zapcore "go.uber.org/zap/zapcore"
)

/*{"access_token": "eyJhbGciOiJIUz......sgrKIi8hdFs",
  "refresh_token": "SPrXw5tBE3......KBQ+luWQVY=",
  "token_type": "Bearer",
  "expires_in": 3600,
  "expiration": 1473188353}*/
func TestGenerateIBMToken(t *testing.T) {

	loggerMgr := initZapLog()
	// Make logger avaible everywhere
	zap.ReplaceGlobals(loggerMgr)
	defer loggerMgr.Sync() // flushes buffer, if any
	// logger= loggerMgr.Sugar()
	// loggerDebug("START")

	conf := initConf()
	// loggerDebug("TestRetrieveToken | Retrieving token ...")
	token := GenerateIBMToken(conf.Apikey)
	// loggerDebug("TestRetrieveToken | Token retrieved -> ", token)
	if token == "" {
		t.Fail()
	}
	// loggerDebug("TestRetrieveToken | Lenght -> ", len(token))
	if len(token) != 1106 {
		t.Fail()
	}
}

func TestCreateDB(t *testing.T) {
	loggerMgr := initZapLog()
	// Make logger avaible everywhere
	zap.ReplaceGlobals(loggerMgr)
	defer loggerMgr.Sync() // flushes buffer, if any
	// logger= loggerMgr.Sugar()
	// loggerDebug("START")
	conf := initConf()

	dbName := `test_db`
	// loggerDebug("TestCreateDB | Creating a new DB ...")
	if !CreateDB(conf.Token, dbName, conf.DBUrl, false) {
		t.Fail()
		return
	}
	// loggerDebug("TestCreateDB | DB [", dbName, "] created succesfully")

	// loggerDebug("TestCreateDB | Creating a new DB with same name")
	if CreateDB(conf.Token, dbName, conf.DBUrl, false) {
		t.Fail()
		return
	}
	// loggerDebug("TestCreateDB | DB [", dbName, "] unable (succesfully) to create")
}

func TestRemoveDB(t *testing.T) {
	loggerMgr := initZapLog()
	// Make logger avaible everywhere
	zap.ReplaceGlobals(loggerMgr)
	defer loggerMgr.Sync() // flushes buffer, if any
	// logger= loggerMgr.Sugar()
	// loggerDebug("START")
	conf := initConf()

	// loggerDebug("Conf -> ", conf)
	dbName := `test_db`
	// loggerDebug("TestRemoveDB | Removing an existent DB ...")
	if !RemoveDB(conf.Token, dbName, conf.DBUrl) {
		t.Fail()
		return
	}
	// loggerDebug("TestRemoveDB | DB [", dbName, "] removed succesfully")

	// loggerDebug("TestRemoveDB | Trying to remove same DB ...")
	if RemoveDB(conf.Token, dbName, conf.DBUrl) {
		t.Fail()
		return
	}
	// loggerDebug("TestRemoveDB | DB [", dbName, "] unable (succesfully) to remove")
}
func TestGetDBDetails(t *testing.T) {}
func TestGetAllDBs(t *testing.T) {
	loggerMgr := initZapLog()
	// Make logger avaible everywhere
	zap.ReplaceGlobals(loggerMgr)
	defer loggerMgr.Sync() // flushes buffer, if any
	// logger= loggerMgr.Sugar()
	// loggerDebug("START")
	conf := initConf()

	// loggerDebug("Conf -> ", conf)
	dbs := GetAllDBs(conf.Apikey, conf.DBUrl)
	// loggerDebug("TestRemoveDB | DBs [", dbs, "] removed succesfully")
	if dbs == nil {
		t.Fail()
	}
}

func initZapLog() *zap.Logger {
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger, _ := config.Build()
	return logger
}

func initConf() Conf {
	file, _ := ioutil.ReadFile("conf.json")
	var conf Conf
	fmt.Println(file)
	err := json.Unmarshal([]byte(file), &conf)
	if err != nil {
		fmt.Println("ERROR! File not found ", err)
		os.Exit(0)
	}
	fmt.Println(conf)
	return conf
}

func TestGenerateCookie(t *testing.T) {

	conf := initConf()

	GenerateCookie(`https://`+conf.Host, conf.Username, conf.Password)
}

// func TestGetAllDocuments(t *testing.T) {}
// func TestInsertDocument(t *testing.T)     {}
// func TestGetDocument(t *testing.T)        {}
// func TestUpdateDocument(t *testing.T)     {}
// func TestDeleteDocument(t *testing.T)     {}
// func TestInsertBulkDocument(t *testing.T) {}
