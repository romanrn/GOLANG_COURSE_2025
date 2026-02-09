package main

import (
	"fmt"
	"lesson_14/internal/database/mongo"
	"lesson_14/internal/handlers"
	"lesson_14/internal/services"
	"log/slog"
	"os"
	"strconv"
	"time"

	"lesson_14/internal/server"

	gomongo "go.mongodb.org/mongo-driver/mongo"
)

const (
	serverHostEnv                  = "SERVER_HOST"
	serverPortEnv                  = "SERVER_PORT"
	readTimeoutEnv                 = "READ_TIMEOUT"
	writeTimeoutEnv                = "WRITE_TIMEOUT"
	shutDownTimeoutEnv             = "SHUTDOWN_TIMEOUT"
	appNameEnv                     = "APP_NAME"
	dbTypeEnv                      = "DB_TYPE"
	dbURIEnv                       = "DB_URI"
	dbNameEnv                      = "DATABASE_NAME"
	dbPortEnv                      = "DB_PORT"
	dbHostEnv                      = "DB_HOST"
	dbUserEnv                      = "DATABASE_USERNAME"
	dbPswEnv                       = "DATABASE_PASSWORD"
	maxPoolSizeEnv                 = "MAX_POOL_SIZE"
	minPoolSizeEnv                 = "MIN_POOL_SIZE"
	maxConnIdleTimeEnv             = "MAX_CONN_IDLE_TIME"
	defaultAppName                 = "Document DB API v1.0"
	defaultPort                    = "8080"
	defaultHost                    = "localhost"
	defaultDbHost                  = "localhost"
	defaultDbPort                  = "27017"
	defaultDBType                  = "mongo"
	defaultDBName                  = "myapp"
	defaultDbURI                   = ""
	defaultDbUser                  = "root"
	defaultDbPsw                   = "root"
	defaultReadTimeout             = 10 * time.Second
	defaultWriteTimeout            = 10 * time.Second
	defaultGracefulShutdownTimeout = 30 * time.Second
	defaultMaxPoolSize             = 100
	defaultMinPoolSize             = 10
	defaultMaxConnIdleTime         = 30 * time.Second
)

func LoadConfig(logger *slog.Logger) (*server.ServerConfig, error) {
	readTimeout, err := getTimeOut(readTimeoutEnv, logger)
	if err != nil {
		return nil, fmt.Errorf("read timeout: %w", err)
	}

	writeTimeout, err := getTimeOut(writeTimeoutEnv, logger)
	if err != nil {
		return nil, fmt.Errorf("write timeout: %w", err)
	}

	shutdownTimeout, err := getTimeOut(shutDownTimeoutEnv, logger)
	if err != nil {
		return nil, fmt.Errorf("shutdown timeout: %w", err)
	}

	config := &server.ServerConfig{
		Host:            getHost(),
		Port:            getPort(),
		ReadTimeout:     readTimeout,
		WriteTimeout:    writeTimeout,
		ShutdownTimeout: shutdownTimeout,
		AppName:         getAppName(),
		DbType:          getDbType(),
		DbName:          getDbName(),
		DbHost:          getDbHost(),
		DbPort:          getDbPort(),
		DbUri:           getDbUri(),
		DbUserName:      getDbUser(),
		DbPsw:           getDbPsw(),
		MaxPoolSize:     getMaxPoolSize(),
		MinPoolSize:     getMinPoolSize(),
		MaxConnIdleTime: getMaxConnIdleTime(),
	}

	logger.Info("Configuration",
		"host", config.Host,
		"port", config.Port,
		"read_timeout", config.ReadTimeout,
		"write_timeout", config.WriteTimeout,
		"shutdown_timeout", config.ShutdownTimeout,
		"app_name", config.AppName,
		"db_type", config.DbType,
	)

	return config, nil
}

func getHost() string {
	if host := os.Getenv(serverHostEnv); host != "" {
		return host
	}
	return defaultHost
}

func getPort() string {
	if port := os.Getenv(serverPortEnv); port != "" {
		return port
	}
	return defaultPort
}

func getDbHost() string {
	if host := os.Getenv(dbHostEnv); host != "" {
		return host
	}
	return defaultDbHost
}

func getDbPort() string {
	if port := os.Getenv(dbPortEnv); port != "" {
		return port
	}
	return defaultDbPort
}

func getDbUri() string {
	if dbUri := os.Getenv(dbURIEnv); dbUri != "" {
		return dbUri
	}
	return defaultDbURI
}

func getDbUser() string {
	if dbUser := os.Getenv(dbUserEnv); dbUser != "" {
		return dbUser
	}
	return defaultDbUser
}

func getDbPsw() string {
	if dbPsw := os.Getenv(dbPswEnv); dbPsw != "" {
		return dbPsw
	}
	return defaultDbPsw
}

func getMaxPoolSize() uint64 {
	if maxPoolSizeStr := os.Getenv(maxPoolSizeEnv); maxPoolSizeStr != "" {
		maxPoolSize, err := strconv.ParseUint(maxPoolSizeStr, 10, 64)
		if err == nil {
			return maxPoolSize
		}
	}
	return defaultMaxPoolSize
}

func getMinPoolSize() uint64 {
	if minPoolSizeStr := os.Getenv(minPoolSizeEnv); minPoolSizeStr != "" {
		minPoolSize, err := strconv.ParseUint(minPoolSizeStr, 10, 64)
		if err == nil {
			return minPoolSize
		}
	}
	return defaultMinPoolSize
}

func getMaxConnIdleTime() time.Duration {
	if maxConnIdleTime := os.Getenv(maxConnIdleTimeEnv); maxConnIdleTime != "" {
		idleTime, err := time.ParseDuration(maxConnIdleTime)
		if err == nil {
			return idleTime
		}
	}
	return defaultMaxConnIdleTime
}

func getTimeOut(key string, logger *slog.Logger) (time.Duration, error) {
	if timeOutStr := os.Getenv(key); timeOutStr != "" {
		timeout, err := time.ParseDuration(timeOutStr)
		if err != nil {
			logger.Warn("Invalid timeout value", "key", key, "value", timeOutStr, "error", err)
		} else {
			return timeout, nil
		}
	}
	switch key {
	case readTimeoutEnv:
		return defaultReadTimeout, nil
	case writeTimeoutEnv:
		return defaultWriteTimeout, nil
	case shutDownTimeoutEnv:
		return defaultGracefulShutdownTimeout, nil
	default:

		return 0, fmt.Errorf("unknown timeout key: %s", key)
	}
}

func getAppName() string {
	if appName := os.Getenv(appNameEnv); appName != "" {
		return appName
	}
	return defaultAppName
}

func getDbType() string {
	if dbType := os.Getenv(dbTypeEnv); dbType != "" {
		return dbType
	}
	return defaultDBType
}

func getDbName() string {
	if dbName := os.Getenv(dbNameEnv); dbName != "" {
		return dbName
	}
	return defaultDBName
}

func setupHandlers(appName, dbName string, logger *slog.Logger, dbClient interface{}, dbType string) []server.HandlerRegistrar {
	var collectionRepo services.CollectionRepository
	var documentRepo services.DocumentRepository
	var indexRepo services.IndexRepository

	switch dbType {
	case defaultDBType:
		mongoClient, ok := dbClient.(*gomongo.Client)
		if !ok {
			logger.Error("Failed to cast dbClient to MongoDB client")
			os.Exit(1)
		}
		collectionRepo = mongo.NewMongoCollectionRepository(mongoClient, dbName, logger)
		documentRepo = mongo.NewMongoDocumentRepository(mongoClient, dbName, logger)
		indexRepo = mongo.NewMongoIndexRepository(mongoClient, dbName, logger)

	default:
		logger.Error("Unsupported database type", "type", dbType)
		os.Exit(1)
	}

	collectionService := services.NewCollectionService(logger, collectionRepo)
	documentService := services.NewDocumentService(logger, documentRepo)
	indexService := services.NewIndexService(logger, indexRepo)

	return []server.HandlerRegistrar{
		handlers.NewHealthHandler(appName),
		handlers.NewCollectionHandler(appName, logger, collectionService),
		handlers.NewDocumentHandler(logger, documentService),
		handlers.NewIndexHandler(logger, indexService),
	}
}

func main() {

	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(h).With(slog.String("component", "main_server"))

	logger.Info("=== Server ===")
	config, err := LoadConfig(logger)
	if err != nil {
		logger.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	dbClient, err := server.SetupDatabaseWithCleanup(config, logger)
	if err != nil {
		logger.Error("Database initialization failed", "error", err)
		os.Exit(1)
	}

	allHandlers := setupHandlers(config.AppName, config.DbName, logger, dbClient, config.DbType)

	// Создаём сервер
	srv := server.NewServer(config, logger, allHandlers...)

	// Run server with graceful shutdown
	srv.Run()
}
