package main

import (
	"fmt"
	"time"

	"github.com/jianhan/ms-bmp-elasticsearch/runners"
	cfgreader "github.com/jianhan/pkg/configs"
	"github.com/micro/go-micro"
	"github.com/nats-io/go-nats-streaming"
	"github.com/spf13/viper"
)

func main() {
	serviceConfigs, err := cfgreader.NewReader(viper.GetString("ENVIRONMENT")).Read()
	if err != nil {
		panic(fmt.Sprintf("error while reading configurations: %s", err.Error()))
	}

	// initialize new service
	srv := micro.NewService(
		micro.Name(serviceConfigs.Name),
		micro.RegisterTTL(time.Duration(serviceConfigs.RegisterTTL)*time.Second),
		micro.RegisterInterval(time.Duration(serviceConfigs.RegisterInterval)*10),
		micro.Version(serviceConfigs.Version),
		micro.Metadata(serviceConfigs.Metadata),
	)

	// init service
	srv.Init()

	sc, err := stan.Connect(viper.GetString("NATS_STREAMING_CLUSTER"), viper.GetString("NATS_STREAMING_CLIENT_ID"))
	if err != nil {
		panic(err)
	}
	defer sc.Close()
	suppliersRunner := runners.NewSuppliersRunner(sc)
	suppliersRunner.Run()

	if err := srv.Run(); err != nil {
		panic(err)
	}
}

func init() {
	viper.SetDefault("ENVIRONMENT", "development")
	viper.SetDefault("NATS_STREAMING_CLUSTER", "test-cluster")
	viper.SetDefault("NATS_STREAMING_CLIENT_ID", "elasticsearch-client")
}
