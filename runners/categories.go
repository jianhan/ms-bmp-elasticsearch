package runners

import (
	"context"

	"github.com/davecgh/go-spew/spew"
	"github.com/gogo/protobuf/proto"
	pcategories "github.com/jianhan/ms-bmp-products/proto/categories"
	"github.com/nats-io/go-nats-streaming"
	"github.com/olivere/elastic"
	"github.com/sirupsen/logrus"
)

type categoriesRunner struct {
	stanConn      stan.Conn
	elasticClient *elastic.Client
	index         string
	topic         string
	base
}

func NewCategoriesRunner(ctx context.Context, topic string, stanConn stan.Conn, elasticClient *elastic.Client, index string) Runner {
	s := &categoriesRunner{
		elasticClient: elasticClient,
		index:         index,
		stanConn:      stanConn,
		topic:         topic,
		base: base{
			elasticClient: elasticClient,
			index:         index,
		},
	}
	if err := s.init(ctx); err != nil {
		logrus.WithError(err).Error("error while init categories runner")
	}

	return s
}
func (r *categoriesRunner) Run() error {
	if _, err := r.stanConn.Subscribe(r.topic, r.sync); err != nil {
		return err
	}

	return nil
}

//func (r *categoriesRunner) Run() error {
//	if _, err := r.stanConn.Subscribe(handlers.TopicCategoriesUpserted, r.sync); err != nil {
//		return err
//	}
//
//	return nil
//}
//
//func (r *categoriesRunner) init(ctx context.Context) error {
//	// check if index exists
//	exists, err := r.elasticClient.IndexExists(r.index).Do(ctx)
//	if err != nil {
//		return err
//	}
//
//	// if index not exists create one
//	if !exists {
//		_, err = r.elasticClient.CreateIndex(r.index).Do(ctx)
//		if err != nil {
//			return err
//		}
//	}
//
//	return nil
//}

func (r *categoriesRunner) sync(msg *stan.Msg) {
	spew.Dump("IN CHILD")
	spew.Dump("RUNNNNNNNN")
	ctx := context.Background()
	// unmarshal response back to native type
	r.elasticClient.DeleteIndex(r.index).Do(ctx)
	rsp := pcategories.UpsertCategoriesRsp{}
	if err := proto.Unmarshal(msg.Data, &rsp); err != nil {
		logrus.WithError(err).WithField("msg", msg.Data).Error("unable to unmarshal response")
	}
	if len(rsp.Categories) == 0 {
		// TODO: log here
		return
	}

	// start to write into documents
	bulkRequest := r.elasticClient.Bulk()
	for _, c := range rsp.Categories {
		req := elastic.NewBulkIndexRequest().Index(r.index).Type(r.index).Id(c.ID).Doc(c)
		bulkRequest = bulkRequest.Add(req)
	}
	_, err := bulkRequest.Do(ctx)
	if err != nil {
		logrus.WithError(err).Errorf("error while trying to bulk sync categories")
	}
}