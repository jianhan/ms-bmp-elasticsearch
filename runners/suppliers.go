package runners

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/jianhan/ms-bmp-products/handlers"
	"github.com/nats-io/go-nats-streaming"
)

type suppliersRunner struct {
	stanConn stan.Conn
}

func NewSuppliersRunner(stanConn stan.Conn) Runner {
	return &suppliersRunner{stanConn: stanConn}
}

func (r *suppliersRunner) Run() error {
	if _, err := r.stanConn.Subscribe(handlers.TopicSuppliersUpserted, r.suppliersUpserted); err != nil {
		return err
	}

	return nil
}

func (r *suppliersRunner) suppliersUpserted(msg *stan.Msg) {
	spew.Dump(msg)
}
