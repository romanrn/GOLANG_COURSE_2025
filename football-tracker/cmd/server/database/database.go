package server

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	gomongo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DatabaseConfig struct {
	DatabaseName string
}

/*func initMongoClient(serverConfig *ServerConfig, logger *slog.Logger) (*gomongo.Client, func() error, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	uri := serverConfig.DbUri
	if uri == "" {
		if serverConfig.DbUserName != "" && serverConfig.DbPsw != "" {
			uri = fmt.Sprintf("mongodb://%s:%s@%s:%s",
				serverConfig.DbUserName,
				serverConfig.DbPsw,
				serverConfig.DbHost,
				serverConfig.DbPort)
		} else {
			uri = fmt.Sprintf("mongodb://%s:%s", serverConfig.DbHost, serverConfig.DbPort)
		}

		logger.Warn("MONGODB_URI not set, using constructed URI", "uri", uri)
	}

	dbName := serverConfig.DbName
	if dbName == "" {
		dbName = "myapp"
		logger.Warn("DATABASE_NAME not set, using default", "name", dbName)
	}

	client, err := gomongo.Connect(ctx,
		options.Client().ApplyURI(uri).SetMaxPoolSize(serverConfig.MaxPoolSize).SetMinPoolSize(serverConfig.MinPoolSize).SetMaxConnIdleTime(serverConfig.MaxConnIdleTime))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	cleanup := func() error {
		if err := client.Disconnect(context.Background()); err != nil {
			logger.Error("Failed to disconnect MongoDB", "error", err)
			return err
		}
		logger.Info("MongoDB client disconnected")
		return nil
	}

	logger.Info("Connected to MongoDB", "uri", uri)
	return client, cleanup, nil
}*/
/*
func InitDatabaseClient(serverConfig *ServerConfig, logger *slog.Logger) (interface{}, func() error, error) {
	switch serverConfig.DbType {
	case "mongo":
		mongoClient, cleanup, err := initMongoClient(serverConfig, logger)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to initialize MongoDB: %w", err)
		}
		return mongoClient, cleanup, nil
	default:
		return nil, nil, fmt.Errorf("unsupported database type: %s", serverConfig.DbType)
	}
}

func SetupDatabaseWithCleanup(serverConfig *ServerConfig, logger *slog.Logger) (interface{}, error) {
	client, cleanup, err := InitDatabaseClient(serverConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan

		if err := cleanup(); err != nil {
			logger.Error("Database cleanup failed", "error", err)
		}
	}()

	return client, nil
}
*/
