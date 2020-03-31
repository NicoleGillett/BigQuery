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

func TextTableChecker_Valid(t *testing.T) {
	//arrange
	g := NewGomegaWithT(t)
	service := "test_service_v1_0"
	//act
	tableExist := TableChecker(service)
	//assert
	g.Expect(tableExist).To(Equal(true))
}