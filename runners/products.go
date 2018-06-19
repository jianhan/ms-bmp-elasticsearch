package runners

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/jianhan/ms-bmp-products/handlers"
	pproducts "github.com/jianhan/ms-bmp-products/proto/products"
	"github.com/nats-io/go-nats-streaming"
	"github.com/olivere/elastic"
	"github.com/sirupsen/logrus"
)

type productsRunner struct {
	stanConn      stan.Conn
	elasticClient *elastic.Client
	index         string
	topic         string
	base
}

func NewProductsRunner(ctx context.Context, topic string, stanConn stan.Conn, elasticClient *elastic.Client, index string) Runner {
	s := &productsRunner{
		stanConn:      stanConn,
		elasticClient: elasticClient,
		index:         index,
		topic:         topic,
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
	if _, err := r.stanConn.Subscribe(handlers.TopicProductsUpserted, r.sync); err != nil {
		return err
	}

	return nil
}

func (r *productsRunner) sync(msg *stan.Msg) {
	ctx := context.Background()
	// unmarshal response back to native type
	r.elasticClient.DeleteIndex(r.index).Do(ctx)
	rsp := pproducts.UpsertProductsRsp{}
	if err := proto.Unmarshal(msg.Data, &rsp); err != nil {
		logrus.WithError(err).WithField("msg", msg.Data).Error("unable to unmarshal response")
	}
	if len(rsp.Products) == 0 {
		// TODO: log here
		return
	}

	// start to write into documents
	bulkRequest := r.elasticClient.Bulk()
	for _, p := range rsp.Products {
		req := elastic.NewBulkIndexRequest().Index(r.index).Type(r.index).Id(p.ID).Doc(p)
		bulkRequest = bulkRequest.Add(req)
	}
	_, err := bulkRequest.Do(ctx)
	if err != nil {
		logrus.WithError(err).Errorf("error while trying to bulk sync products")
	}
}
