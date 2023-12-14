package main

import (
	"context"
	"flag"
	"github.com/clyso/chorus/pkg/config"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/service/worker"
	"github.com/rs/xid"
	"github.com/rs/zerolog"
	stdlog "github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"syscall"
)

// this information will be collected when built, by -ldflags="-X 'main.version=$(tag)' -X 'main.commit=$(commit)'".
var (
	version            = "development"
	commit             = "not set"
	configPath         = flag.String("config", "config/config.yaml", "set path to config directory")
	configOverridePath = flag.String("config-override", "config/override.yaml", "set path to config override directory")
)

func main() {
	flag.Parse()
	var configs []config.Src
	if configPath != nil && *configPath != "" {
		configs = append(configs, config.Path(*configPath))
	}
	if configOverridePath != nil && *configOverridePath != "" {
		configs = append(configs, config.Path(*configOverridePath))
	}

	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGTERM)
	go func() {
		<-signals
		zerolog.Ctx(ctx).Info().Msg("received shutdown signal.")
		cancel()
	}()

	conf, err := worker.GetConfig(configs...)
	if err != nil {
		stdlog.Fatal().Err(err).Msg("critical error. Unable to read app config")
	}

	err = worker.Start(ctx, dom.AppInfo{
		Version: version,
		Commit:  commit,
		App:     "worker",
		AppID:   xid.New().String(),
	}, conf)
	if err != nil {
		stdlog.Err(err).Msg("critical error. Shutdown application")
		os.Exit(1)
	}
}
