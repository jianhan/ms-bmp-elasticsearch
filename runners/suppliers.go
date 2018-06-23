package runners

import (
	"context"

	"github.com/gogo/protobuf/proto"
	psuppliers "github.com/jianhan/ms-bmp-products/proto/suppliers"
	"github.com/nats-io/go-nats-streaming"
	"github.com/olivere/elastic"
	"github.com/sirupsen/logrus"
)

type suppliersRunner struct {
	stanConn      stan.Conn
	elasticClient *elastic.Client
	mapString     string
	index         string
	topic         string
	base
}

func NewSuppliersRunner(ctx context.Context, topic string, stanConn stan.Conn, elasticClient *elastic.Client, index string) Runner {
	s := &suppliersRunner{
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
		logrus.WithError(err).Error("error while init suppliers runner")
	}

	return s
}

func (r *suppliersRunner) Run() error {
	if _, err := r.stanConn.Subscribe(r.topic, r.sync); err != nil {
		return err
	}

	return nil
}

func (r *suppliersRunner) sync(msg *stan.Msg) {
	ctx := context.Background()
	// unmarshal response back to native type
	r.elasticClient.DeleteIndex(r.index).Do(ctx)
	rsp := psuppliers.UpsertSuppliersRsp{}
	if err := proto.Unmarshal(msg.Data, &rsp); err != nil {
		logrus.WithError(err).WithField("msg", msg.Data).Error("unable to unmarshal response")
	}
	if len(rsp.Suppliers) == 0 {
		// TODO: log here
		return
	}

	// start to write into documents
	bulkRequest := r.elasticClient.Bulk()
	for _, s := range rsp.Suppliers {
		req := elastic.NewBulkIndexRequest().Index(r.index).Type(r.index).Id(s.ID).Doc(s)
		bulkRequest = bulkRequest.Add(req)
	}
	_, err := bulkRequest.Do(ctx)
	if err != nil {
		logrus.WithError(err).Errorf("error while trying to bulk sync suppliers")
	}
}
