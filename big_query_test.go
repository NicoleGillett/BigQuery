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