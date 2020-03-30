package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
)

type BQ struct {
	client *bigquery.Client
}

func NewBigQueryClient(ctx context.Context) (*BQ, error) {
	client, err := bigquery.NewClient(ctx,"cytora-dev-228110")
	if err != nil {
		return nil, err
	}
	return &BQ{
		client: client,
	}, nil
}

func (bq *BQ) Metadata(service string, ctx context.Context) error {
	md, err := bq.client.Dataset("cytora_dev_business_intelligence").Table(service).Metadata(ctx)
	if err != nil {
		return err
	}
	fmt.Println(md.ExternalDataConfig.SourceURIs)
	return nil
}

func main() {
	ctx := context.Background()
	store, err := NewBigQueryClient(ctx)
	if err != nil {
		panic(err)
	}
	err = store.Metadata("address_processing", ctx)
	if err != nil {
		panic(err)
	}
}
