package runners

import (
	"context"

	"github.com/olivere/elastic"
)

type Runner interface {
	Run() error
}

type base struct {
	elasticClient *elastic.Client
	index         string
}

func (r base) init(ctx context.Context) error {
	// check if index exists
	exists, err := r.elasticClient.IndexExists(r.index).Do(ctx)
	if err != nil {
		return err
	}

	// if index not exists create one
	if !exists {
		_, err = r.elasticClient.CreateIndex(r.index).Do(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}
