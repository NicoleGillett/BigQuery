package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
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

func (bq *BQ) TableType(ctx context.Context) error {
	q := bq.client.Query(`
		SELECT DISTINCT metadata.proto_version
    	FROM ` + "`cytora_dev_business_intelligence.properties`" + `
	`)

	it, err := q.Read(ctx)
	if err != nil {
		return err
	}

	err = iter(it)
	if err != nil {
		return err
	}

	return nil
}

func (bq *BQ) TableVersion(ctx context.Context) error {
	q := bq.client.Query(`
		SELECT DISTINCT metadata.proto_version
    	FROM ` + "`cytora_dev_business_intelligence.properties`" + `
	`)

	it, err := q.Read(ctx)
	if err != nil {
		return err
	}

	err = iter(it)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	ctx := context.Background()
	store, err := NewBigQueryClient(ctx)
	if err != nil {
		panic(err)
	}
	//err = store.TableType(ctx)
	err = store.TableVersion(ctx)
	if err != nil {
		panic(err)
	}
}

func iter(it *bigquery.RowIterator) error {
	for {
		var value []bigquery.Value
		err := it.Next(&value)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		fmt.Println(value)
	}
	return nil
}
