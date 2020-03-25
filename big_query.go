package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"github.com/urfave/cli"
)

type BQ struct {
	client *bigquery.Client
}

func bqAction(c *cli.Context) error {
	ctx := context.Background()
	store, err := NewBigQueryClient(ctx)
	if err != nil {
		return err
	}
	err = store.QueryWiki(ctx)
	if err != nil {
		return err
	}
	return nil
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

func (bq *BQ) QueryWiki(ctx context.Context) error {
	q := bq.client.Query(`
	SELECT title
    FROM ` + "`bigquery-public-data.wikipedia.pageviews_2020`" + `
    WHERE DATE(datehour) = "2020-03-25"
    LIMIT 10
	`)
	it, err := q.Read(ctx)
	if err != nil {
		return err
	}

	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		fmt.Println(values)
	}
	return nil
}

func main() {
	ctx := context.Background()
	store, err := NewBigQueryClient(ctx)
	if err != nil {
		panic(err)
	}
	err = store.QueryWiki(ctx)
	if err != nil {
		panic(err)
	}
}