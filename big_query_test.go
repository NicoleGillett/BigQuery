package main

import (
	"testing"
	. "github.com/onsi/gomega"
)

const (
	testURI = "gs://cytora-dev-bi-message-consumer-message-store/valid/test-service/test-type/test-version/*.json"
)

func TestExtractType(t *testing.T) {
	//arrange
	g := NewGomegaWithT(t)
	//act
	typ := ExtractType(testURI)
	//assert
	g.Expect(typ).To(Equal("test-type"))
}

func TestExtractVersion(t *testing.T) {
	//arrange
	g := NewGomegaWithT(t)
	//act
	typ := ExtractVersion(testURI)
	//assert
	g.Expect(typ).To(Equal("test-version"))
}

func TestVersionChecker_Valid(t *testing.T) {
	//arrange
	g := NewGomegaWithT(t)
	tableName := "test_service_v1_0"
	//act
	tableExist := VersionChecker(tableName)
	//assert
	g.Expect(tableExist).To(Equal(true))
}

func TestVersionChecker_Invalid(t *testing.T) {
	//arrange
	g := NewGomegaWithT(t)
	tableName := "test_service"
	//act
	tableExist := VersionChecker(tableName)
	//assert
	g.Expect(tableExist).To(Equal(false))
}

func TestTableMatcher(t *testing.T) {
	//arrange
	g := NewGomegaWithT(t)
	tableName := []string{"test_service_v1_0"}
	service := "test_service"
	//act
	serviceTables := TableMatcher(service, tableName)
	//assert
	g.Expect(serviceTables).To(Equal(tableName))
}
