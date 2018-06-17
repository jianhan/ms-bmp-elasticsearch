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
	ctx := context.Background()
	// unmarshal response back to native type
	r.elasticClient.DeleteIndex(r.index).Do(ctx)
	rsp := psuppliers.UpsertSuppliersRsp{}
	if err := proto.Unmarshal(msg.Data, &rsp); err != nil {
		logrus.WithError(err).WithField("msg", msg.Data).Error("unable to unmarshal response")
	}

	//bulk := r.elasticClient.BulkProcessor().Index().Index(r.index)
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
