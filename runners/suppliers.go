package runners

import (
	"context"

	"github.com/davecgh/go-spew/spew"
	"github.com/jianhan/ms-bmp-products/handlers"
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
	if _, err := r.stanConn.Subscribe(handlers.TopicSuppliersUpserted, r.suppliersUpserted); err != nil {
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

func (r *suppliersRunner) suppliersUpserted(msg *stan.Msg) {

	spew.Dump(msg)
}
