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
	// loggerMgr := initZapLog()
	// zap.ReplaceGlobals(loggerMgr)
	// defer loggerMgr.Sync() // flushes buffer, if any
	// logger := loggerMgr.Sugar()
	// logger.Debug("START")
	conf := initConf()
	// loggerDebug("TestRetrieveToken | Retrieving token ...")
	token := GenerateIBMToken(conf.Apikey)
	if token == "" {
		t.Fail()
	}
	if len(token) != 1106 {
		t.Fail()
	}
}

func TestCreateDB(t *testing.T) {
	conf := initConf()
	dbName := `test_db`
	if !CreateDB(conf.Token, dbName, conf.DBUrl, false) {
		t.Fail()
		return
	}
	if CreateDB(conf.Token, dbName, conf.DBUrl, false) {
		t.Fail()
		return
	}
}

func TestRemoveDB(t *testing.T) {
	conf := initConf()
	dbName := `test_db`
	if !RemoveDB(conf.Token, dbName, conf.DBUrl) {
		t.Fail()
		return
	}
	if RemoveDB(conf.Token, dbName, conf.DBUrl) {
		t.Fail()
		return
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
	err := json.Unmarshal([]byte(file), &conf)
	if err != nil {
		fmt.Println("ERROR! File not found ", err)
		os.Exit(0)
	}
	return conf
}

func TestGenerateCookie(t *testing.T) {
	conf := initConf()
	cookie := GenerateCookie(`https://`+conf.Host, conf.Username, conf.Password)
	if cookie == "" {
		t.Fail()
	}
}

// func TestGetAllDBs(t *testing.T) {
// 	conf := initConf()
// 	dbs := GetAllDBs(conf.Apikey, conf.DBUrl)
// 	if dbs == nil {
// 		t.Fail()
// 	}
// }
