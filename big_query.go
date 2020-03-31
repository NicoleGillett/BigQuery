package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"strings"
)

const (
	dataset = "cytora_dev_business_intelligence"
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

func (bq *BQ) Tables(dataset string, ctx context.Context) ([]string, error) {
	//return tables from dataset
	it := bq.client.Dataset(dataset).Tables(ctx)
	_ = it
	p := make([]string, 0)
	for {
		attrs, err := it.Next()

		if err == iterator.Done {
			break
		}

		if err != nil {
			return p, err
		}

		p = append(p, attrs.TableID)
	}
	return p, nil
}

func (bq *BQ) Metadata(dataset, service string, ctx context.Context) error {
	// retrieve metadata from BigQuery
	md, err := bq.client.Dataset(dataset).Table(service).Metadata(ctx)
	if err != nil {
		panic(err)
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
	tables, err := store.Tables(dataset, ctx)
	if err != nil {
		panic(err)
	}
	err = store.Metadata(dataset, "postcode-validation", ctx)
	if err != nil {
		panic(err)
	}
}

func ExtractType(uri string) string {
	splitString := strings.Split(uri, "/")
	return splitString[5]
}

func ExtractVersion(uri string) string {
	splitString := strings.Split(uri, "/")
	return splitString[6]
}
