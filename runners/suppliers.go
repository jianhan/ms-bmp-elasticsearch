package runners

import (
	"context"

	"io/ioutil"

	"github.com/gogo/protobuf/proto"
	"github.com/jianhan/ms-bmp-products/handlers"
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
}

func NewSuppliersRunner(stanConn stan.Conn, elasticClient *elastic.Client, index string) Runner {
	s := &suppliersRunner{stanConn: stanConn, elasticClient: elasticClient, index: index}
	if err := s.init(context.Background()); err != nil {
		logrus.WithError(err).Error("error while init suppliers runner")
	}

	return s
}

func (r *suppliersRunner) Run() error {
	if _, err := r.stanConn.Subscribe(handlers.TopicSuppliersUpserted, r.sync); err != nil {
		return err
	}

	return nil
}

func (r *suppliersRunner) init(ctx context.Context) error {
	// read map
	mapString, err := ioutil.ReadFile("./runners/supplier_map.json")
	if err != nil {
		return err
	}
	r.mapString = string(mapString)

	// check if index exists
	exists, err := r.elasticClient.IndexExists(r.index).Do(ctx)
	if err != nil {
		return err
	}

	// if index not exists create one
	if !exists {
		_, err = r.elasticClient.CreateIndex(r.index).BodyString(string(mapString)).Do(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *suppliersRunner) sync(msg *stan.Msg) {
	r.elasticClient.DeleteIndex(r.index).Do(context.Background())
	rsp := psuppliers.UpsertSuppliersRsp{}
	if err := proto.Unmarshal(msg.Data, &rsp); err != nil {
		logrus.WithError(err).WithField("msg", msg.Data).Error("unable to unmarshal response")
	}

	r.elasticClient.Bulk().Index(r.index)
}
