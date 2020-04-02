package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"regexp"
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
		//TODO: write test to check that only versioned tables are added.
		if VersionChecker(attrs.TableID) {
			p = append(p, attrs.TableID)
		}
	}
	return p, nil
}

func (bq *BQ) TypeVersion(serviceTables []string, ctx context.Context) map[string][]string {
	m := make(map[string][]string)
	for _, table := range serviceTables {
		md, err := bq.client.Dataset(dataset).Table(table).Metadata(ctx)
		if err != nil {
			panic(err)
		}
		uri := md.ExternalDataConfig.SourceURIs
		typ := ExtractType(uri[0])
		version := ExtractVersion(uri[0])
		m[typ] = append(m[typ], version)
	}
	return m
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
	serviceTables := TableMatcher("properties", tables)
	typeVersion := store.TypeVersion(serviceTables, ctx)
	fmt.Println(typeVersion)
}

func ExtractType(uri string) string {
	splitString := strings.Split(uri, "/")
	return splitString[5]
}

func ExtractVersion(uri string) string {
	splitString := strings.Split(uri, "/")
	return splitString[6]
}

func VersionChecker(tableName string) bool {
	matched, err := regexp.MatchString(`(?m)v\d_\d`, tableName)
	if err != nil {
		panic(err)
	}
	return matched
}

func TableMatcher(service string, tables []string) []string {
	var serviceTables []string
	for _, table := range tables {
		if table[:len(table)-5] == service {
			serviceTables = append(serviceTables, table)
		}
	}
	return serviceTables
}


