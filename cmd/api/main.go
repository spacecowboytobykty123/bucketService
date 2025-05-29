package main

import (
	"bucketService/internal/app/grpcapp"
	subsgrpc "bucketService/internal/clients/subscription/grpc"
	toygrpc "bucketService/internal/clients/toys/grpc"
	"bucketService/internal/jsonlog"
	"bucketService/internal/services/bucket"
	"bucketService/storage/postgres"
	"context"
	"flag"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	_ "github.com/lib/pq"
	bckt "github.com/spacecowboytobykty123/bucketProto/gen/go/bucket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const version = "1.0.0"

type StorageDetails struct {
	DSN          string
	MaxOpenConns int
	MaxIdleConns int
	MaxIdleTime  string
}

type Client struct {
	Address      int           `yaml:"address"`
	Timeout      time.Duration `yaml:"timeout"`
	RetriesCount int           `yaml:"retries_count"`
	insecure     bool          `yaml:"insecure"`
}

type ClientsConfig struct {
	Subs Client `yaml:"subs"`
	Toys Client
}

type GRPCConfig struct {
	Port    int
	Timeout time.Duration
}

type Config struct {
	env       string
	DB        StorageDetails
	GRPC      GRPCConfig
	TokenTTL  time.Duration
	Clients   ClientsConfig
	AppSecret string
}

type Application struct {
	GRPCSrv *grpcapp.App
}

func main() {
	var cfg Config
	flag.StringVar(&cfg.env, "env", "development", "Environment (development|staging|production)")
	flag.StringVar(&cfg.DB.DSN, "db-dsn", "postgres://bucket:pass@localhost:5432/buckets?sslmode=disable&client_encoding=UTF8", "PostgresSQL DSN")
	flag.IntVar(&cfg.DB.MaxOpenConns, "db-max-open-conns", 25, "PostgresSQL max open connections")
	flag.IntVar(&cfg.DB.MaxIdleConns, "db-max-Idle-conns", 25, "PostgresSQL max Idle connections")
	flag.StringVar(&cfg.DB.MaxIdleTime, "db-max-Idle-time", "15m", "PostgresSQl max Idle time")

	flag.IntVar(&cfg.GRPC.Port, "grpc-port", 2000, "grpc-port")
	flag.DurationVar(&cfg.TokenTTL, "token-ttl", time.Hour, "GRPC's work duration")
	flag.IntVar(&cfg.Clients.Subs.Address, "sub-client-addr", 3000, "sub-port")
	logger := jsonlog.New(os.Stdout, jsonlog.LevelInfo)
	subsClient, err := subsgrpc.New(context.Background(), logger, cfg.Clients.Subs.Address, cfg.Clients.Subs.Timeout, cfg.Clients.Subs.RetriesCount)
	toysClient, err := toygrpc.New(context.Background(), logger, cfg.Clients.Toys.Timeout, cfg.Clients.Toys.Address)
	if err != nil {
		logger.PrintError(err, map[string]string{
			"message": "failed ot init subs client",
		})
		os.Exit(1)
	}
	flag.Parse()

	app := New(logger, cfg.GRPC.Port, cfg, cfg.TokenTTL, subsClient, toysClient)
	logger.PrintInfo("connection pool established", map[string]string{
		"port": strconv.Itoa(cfg.GRPC.Port),
	})
	go app.GRPCSrv.MustRun()
	go runHTTP(cfg.GRPC.Port, logger)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	sign := <-stop
	logger.PrintInfo("stopping application", map[string]string{
		"signal": sign.String(),
	})

	app.GRPCSrv.Stop()
}

func New(log *jsonlog.Logger, grpcPort int, cfg Config, tokenTTL time.Duration, subsClient *subsgrpc.Client, toyClient *toygrpc.ToyClient) *Application {
	dbCfg := postgres.StorageDetails(cfg.DB)
	db, err := postgres.OpenDB(dbCfg)
	if err != nil {
		log.PrintFatal(err, nil)
	}

	bucketService := bucket.New(log, db, tokenTTL, subsClient, toyClient)
	grpcApp := grpcapp.New(log, grpcPort, bucketService)

	return &Application{GRPCSrv: grpcApp}
}

func runHTTP(grpcPort int, logger *jsonlog.Logger) {
	ctx := context.Background()
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	endpoint := "localhost:" + strconv.Itoa(grpcPort)
	if err := bckt.RegisterBucketHandlerFromEndpoint(ctx, mux, endpoint, opts); err != nil {
		logger.PrintFatal(err, map[string]string{
			"message": "failed to start HTTP gateway",
			"method":  "main.runHTTP",
		})
	}
	fs := http.FileServer(http.Dir("C:\\Users\\Еркебулан\\GolandProjects\\bucketProto\\gen\\swagger"))
	http.Handle("/swagger/", http.StripPrefix("/swagger/", fs))
	http.Handle("/", mux)

	logger.PrintInfo("HTTP REST gateway and Swagger docs started", map[string]string{
		"port": "2020",
	})

	if err := http.ListenAndServe(":2020", mux); err != nil {
		logger.PrintFatal(err, map[string]string{
			"message": "HTTP gateway crashed",
		})
	}
}
