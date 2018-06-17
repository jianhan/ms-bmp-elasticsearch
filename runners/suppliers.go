package runners

import (
	"context"

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
}

func NewSuppliersRunner(stanConn stan.Conn, elasticClient *elastic.Client) Runner {
	s := &suppliersRunner{stanConn: stanConn, elasticClient: elasticClient}
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
	exists, err := r.elasticClient.IndexExists("suppliers").Do(ctx)
	if err != nil {
		return err
	}

	// if index not exists create one
	if !exists {
		_, err = r.elasticClient.CreateIndex("suppliers").Do(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *suppliersRunner) sync(msg *stan.Msg) {
	r.elasticClient.DeleteIndex("suppliers").Do(context.Background())
	rsp := psuppliers.UpsertSuppliersRsp{}
	if err := proto.Unmarshal(msg.Data, &rsp); err != nil {
		logrus.WithError(err).WithField("msg", msg.Data).Error("unable to unmarshal response")
	}
}
