package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/tokenbankteam/token_issue_stat_task/config"
	"github.com/tokenbankteam/token_issue_stat_task/service"
)

func main() {
	configPath := flag.String("config", "config.yaml", "config file path")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatal().Err(err).Msg("load config failed")
	}
	setupLog(cfg.Log.Level)

	log.Info().Str("config", *configPath).Msg("config loaded")

	db, err := gorm.Open(mysql.Open(cfg.DB.DSN()), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		log.Fatal().Err(err).Msg("connect mysql failed")
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal().Err(err).Msg("get sql db failed")
	}
	sqlDB.SetMaxOpenConns(cfg.DB.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.DB.MaxIdleConns)
	log.Info().
		Str("host", cfg.DB.Host).
		Str("database", cfg.DB.Database).
		Int("maxOpenConns", cfg.DB.MaxOpenConns).
		Msg("mysql connected")

	if !cfg.Scanner.Enabled {
		log.Warn().Msg("scanner disabled, exit")
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	scanner := service.New(cfg, db)
	defer func() {
		if err := scanner.Close(); err != nil {
			log.Error().Err(err).Msg("close kafka writer failed")
		}
	}()

	if err := scanner.Run(ctx); err != nil {
		log.Fatal().Err(err).Msg("scanner run failed")
	}
}

func setupLog(level string) {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	lv, err := zerolog.ParseLevel(level)
	if err != nil || level == "" {
		lv = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(lv)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
}
