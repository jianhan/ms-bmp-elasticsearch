package main

import (
	"fmt"
	"time"

	"context"

	"github.com/jianhan/ms-bmp-elasticsearch/runners"
	"github.com/jianhan/ms-bmp-products/handlers"
	cfgreader "github.com/jianhan/pkg/configs"
	"github.com/micro/go-micro"
	"github.com/nats-io/go-nats-streaming"
	"github.com/olivere/elastic"
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

	ctx := context.Background()

	// elastic client
	elasticClient, err := elastic.NewClient()
	if err != nil {
		panic(err)
	}

	// Ping the Elasticsearch server to get e.g. the version number
	info, code, err := elasticClient.Ping(fmt.Sprintf("%s:%s", viper.GetString("ELASTICSEARCH_HOST"), viper.GetString("ELASTICSEARCH_PORT"))).Do(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)

	// nats streaming
	sc, err := stan.Connect(viper.GetString("NATS_STREAMING_CLUSTER"), viper.GetString("NATS_STREAMING_CLIENT_ID"))
	if err != nil {
		panic(err)
	}
	defer sc.Close()

	// start runners
	suppliersRunner := runners.NewSuppliersRunner(ctx, handlers.TopicSyncSuppliersToElasticSearch, sc, elasticClient, "bmp.suppliers")
	if err := suppliersRunner.Run(); err != nil {
		panic(err)
	}

	// products runner
	productsRunner := runners.NewProductsRunner(ctx, handlers.TopicProductsUpserted, sc, elasticClient, "bmp.products")
	if err := productsRunner.Run(); err != nil {
		panic(err)
	}

	// categories runner
	categoriesRunner := runners.NewCategoriesRunner(ctx, handlers.TopicSyncCategoriesToElasticSearch, sc, elasticClient, "bmp.categories")
	if err := categoriesRunner.Run(); err != nil {
		panic(err)
	}

	if err := srv.Run(); err != nil {
		panic(err)
	}
}

func init() {
	viper.SetDefault("ENVIRONMENT", "development")
	viper.SetDefault("NATS_STREAMING_CLUSTER", "test-cluster")
	viper.SetDefault("NATS_STREAMING_CLIENT_ID", "elasticsearch-client")
	viper.SetDefault("ELASTICSEARCH_HOST", "http://127.0.0.1")
	viper.SetDefault("ELASTICSEARCH_PORT", "9200")
}
