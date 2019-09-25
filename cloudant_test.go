package cloudant

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestInitAuth(t *testing.T) {
	// loggerMgr := initZapLog()
	// zap.ReplaceGlobals(loggerMgr)
	// defer loggerMgr.Sync() // flushes buffer, if any
	// logger := loggerMgr.Sugar()
	// logger.Debug("START")
	conf := initConf()
	auth := conf.InitAuth()
	t.Log("BasicAuth " + auth.BasicAuth)
	t.Log("SessioCookie " + auth.SessionCookie)
	t.Log("IAMToken " + auth.IAMToken)
	t.Log("URL ->" + auth.DBUrl)
	if auth.BasicAuth == "" {
		t.Fail()
	}
}

func TestGetSessionInfo(t *testing.T) {
	conf := initConf()
	auth := conf.InitAuth()
	data := auth.GetSessionInfo()
	t.Log("SessionInfo -> ", data)
	if data == "" {
		t.Fail()
	}
}

func TestGenerateIBMToken(t *testing.T) {
	conf := initConf()
	token := conf.GenerateIBMToken()
	t.Log("Token retrieved -> ", token)
	if token == "" {
		t.Fail()
	}
	if len(token) != 1106 {
		t.Fail()
	}
}

func TestGenerateCookie(t *testing.T) {
	conf := initConf()
	cookie := conf.GenerateCookie(`https://` + conf.Host)
	if cookie == "" {
		t.Fail()
	}
}

func TestPingCloudant(t *testing.T) {}

func TestCreateDB(t *testing.T) {

	conf := initConf()
	auth := conf.InitAuth()
	dbName := `test_db`
	if !auth.CreateDB(dbName, false) {
		t.Fail()
	}
	if auth.CreateDB(dbName, false) {
		t.Fail()
	}
}

func TestGetDBDetails(t *testing.T) {}
func TestGetAllDBs(t *testing.T) {
	// loggerMgr := initZapLog()
	// zap.ReplaceGlobals(loggerMgr)
	// defer loggerMgr.Sync() // flushes buffer, if any
	// logger := loggerMgr.Sugar()
	// logger.Debug("START")
	conf := initConf()
	auth := conf.InitAuth()
	var data []string
	data = auth.GetAllDBs(conf.URL)
	t.Log("All dbs -> ", data)
	if data == nil {
		t.Fail()
	}
}
func TestGetAllDocuments(t *testing.T)    {}
func TestInsertDocument(t *testing.T)     {}
func TestGetDocument(t *testing.T)        {}
func TestUpdateDocument(t *testing.T)     {}
func TestDeleteDocument(t *testing.T)     {}
func TestInsertBulkDocument(t *testing.T) {}

func TestRemoveDB(t *testing.T) {
	loggerMgr := initZapLog()
	zap.ReplaceGlobals(loggerMgr)
	defer loggerMgr.Sync() // flushes buffer, if any
	logger := loggerMgr.Sugar()
	logger.Debug("START")
	conf := initConf()
	auth := conf.InitAuth()
	dbName := `test_db`
	if !auth.RemoveDB(dbName) {
		t.Error("Unable to remove DB ", dbName)
		t.Fail()
	}
	if auth.RemoveDB(dbName) {
		t.Error("Expected error during removing!")
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
