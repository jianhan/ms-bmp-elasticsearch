package runners

import (
	"context"

	"github.com/jianhan/ms-bmp-products/handlers"
	pproducts "github.com/jianhan/ms-bmp-products/proto/products"
	"github.com/nats-io/go-nats-streaming"
	"github.com/olivere/elastic"
	"github.com/sirupsen/logrus"
)

type productsRunner struct {
	stanConn              stan.Conn
	elasticClient         *elastic.Client
	productsServiceClient pproducts.ProductsServiceClient
	index                 string
	topic                 string
	base
}

func NewProductsRunner(ctx context.Context, productsServiceClient pproducts.ProductsServiceClient, topic string, stanConn stan.Conn, elasticClient *elastic.Client, index string) Runner {
	s := &productsRunner{
		stanConn:      stanConn,
		elasticClient: elasticClient,
		index:         index,
		topic:         topic,
		productsServiceClient: productsServiceClient,
		base: base{
			elasticClient: elasticClient,
			index:         index,
		},
	}
	if err := s.init(ctx); err != nil {
		logrus.WithError(err).Error("error while init products runner")
	}

	return s
}

func (r *productsRunner) Run() error {
	if _, err := r.stanConn.Subscribe(handlers.TopicSyncProductsToElasticSearch, r.sync); err != nil {
		return err
	}

	return nil
}

func (r *productsRunner) sync(msg *stan.Msg) {
	ctx := context.Background()
	// unmarshal response back to native type
	r.elasticClient.DeleteIndex(r.index).Do(ctx)

	rsp, err := r.productsServiceClient.Products(context.Background(), &pproducts.ProductsReq{})
	if err != nil {
		logrus.WithError(err).Error("unable to get products")
		return
	}

	// start to write into documents
	bulkRequest := r.elasticClient.Bulk()
	for _, p := range rsp.Products {
		req := elastic.NewBulkIndexRequest().Index(r.index).Type(r.index).Id(p.ID).Doc(p)
		bulkRequest = bulkRequest.Add(req)
	}
	_, err = bulkRequest.Do(ctx)
	if err != nil {
		logrus.WithError(err).Errorf("error while trying to bulk sync products")
	}
}
