package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type configureTestCase struct {
	args     []string
	env      func()
	expected *Configuration
}

const (
	fiveSecond         = 5 * time.Second
	thirtySecond       = 30 * time.Second
	oneHundredMegaByte = 1024 * 100
)

var configuresCases = []*configureTestCase{
	{ // case 1: no environment variable, no argument
		args: []string{},
		expected: &Configuration{
			Host:              "localhost",
			Port:           8080,
			GracefulPeriod:    thirtySecond,
			ConnectionTimeOut: fiveSecond,
			EmitterWorker:     5,
			RecieveWorker:     10,
			TemporayFolder:    "data",
			BufferSize:        ByteSize{raw: "100MB", Size: oneHundredMegaByte},
		},
	},
	{ // case 2: define host environment variable
		args: []string{}, env: func() {
			os.Setenv("HOST", "10.0.0.1")
		},
		expected: &Configuration{
			Host:              "10.0.0.1",
			Port:           8080,
			GracefulPeriod:    thirtySecond,
			ConnectionTimeOut: fiveSecond,
			EmitterWorker:     5,
			RecieveWorker:     10,
			TemporayFolder:    "data",
			BufferSize:        ByteSize{raw: "100MB", Size: oneHundredMegaByte},
		},
	},
	{ // case 3: define port environment variable
		args: []string{}, env: func() {
			os.Setenv("PORT", "9092")
		},
		expected: &Configuration{
			Host:              "localhost",
			Port:           9092,
			GracefulPeriod:    thirtySecond,
			ConnectionTimeOut: fiveSecond,
			EmitterWorker:     5,
			RecieveWorker:     10,
			TemporayFolder:    "data",
			BufferSize:        ByteSize{raw: "100MB", Size: oneHundredMegaByte},
		},
	},
	{ // case 4: define port environment variable
		args: []string{}, env: func() {
			os.Setenv("PORT", "4091")
			os.Setenv("HOST", "10.0.2.4")
			os.Setenv("BUFFER", "12GB")
		},
		expected: &Configuration{
			Host:              "10.0.2.4",
			Port:           4091,
			GracefulPeriod:    thirtySecond,
			ConnectionTimeOut: fiveSecond,
			EmitterWorker:     5,
			RecieveWorker:     10,
			TemporayFolder:    "data",
			BufferSize:        ByteSize{raw: "12GB", Size: 12 * 1024 * 1024},
		},
	},
	{ // case 5
		args: []string{"-port", "3921", "-buffer", "60mb"},
		expected: &Configuration{
			Host:              "localhost",
			Port:           3921,
			GracefulPeriod:    thirtySecond,
			ConnectionTimeOut: fiveSecond,
			EmitterWorker:     5,
			RecieveWorker:     10,
			TemporayFolder:    "data",
			BufferSize:        ByteSize{raw: "60mb", Size: 60 * 1024},
		},
	},
	{ // case 6
		args: []string{"-port", "4351", "-host", "example.com"},
		expected: &Configuration{
			Host:              "example.com",
			Port:           4351,
			GracefulPeriod:    thirtySecond,
			ConnectionTimeOut: fiveSecond,
			EmitterWorker:     5,
			RecieveWorker:     10,
			TemporayFolder:    "data",
			BufferSize:        ByteSize{raw: "100MB", Size: oneHundredMegaByte},
		},
	},
	{ // case 7
		args: []string{"-port", "4351"},
		env:  func() { os.Setenv("HOST", "example.com") },
		expected: &Configuration{
			Host:              "example.com",
			Port:           4351,
			GracefulPeriod:    thirtySecond,
			ConnectionTimeOut: fiveSecond,
			EmitterWorker:     5,
			RecieveWorker:     10,
			TemporayFolder:    "data",
			BufferSize:        ByteSize{raw: "100MB", Size: oneHundredMegaByte},
		},
	},
	{ // case 8: argument override host and port
		args: []string{"-port", "4351", "-host", "10.2.1.1", "-timeout", "10"},
		env: func() {
			os.Setenv("HOST", "example.com")
			os.Setenv("PORT", "8751")
			os.Setenv("GRACEFUL", "20")
		},
		expected: &Configuration{
			Host:              "10.2.1.1",
			Port:           4351,
			GracefulPeriod:    20 * time.Second,
			ConnectionTimeOut: 10 * time.Second,
			EmitterWorker:     5,
			RecieveWorker:     10,
			TemporayFolder:    "data",
			BufferSize:        ByteSize{raw: "100MB", Size: oneHundredMegaByte},
		},
	},
	{ // case 9: argument override host and timeout
		args: []string{"-port", "1276", "-timeout", "12"},
		env: func() {
			os.Setenv("HOST", "example.com")
			os.Setenv("PORT", "1276")
			os.Setenv("GRACEFUL", "20")
			os.Setenv("TIMEOUT", "61")
		},
		expected: &Configuration{
			Host:              "example.com",
			Port:           1276,
			GracefulPeriod:    20 * time.Second,
			ConnectionTimeOut: 12 * time.Second,
			EmitterWorker:     5,
			RecieveWorker:     10,
			TemporayFolder:    "data",
			BufferSize:        ByteSize{raw: "100MB", Size: oneHundredMegaByte},
		},
	},
}

func TestConfigure(t *testing.T) {
	conf := &Configuration{}
	for i, tc := range configuresCases {
		t.Logf("TestConfiguraion case: #%d", i+1)
		if tc.env != nil {
			tc.env()
		}
		conf.init("sample", tc.args)
		assert.Equal(t, tc.expected, conf)
		// unset host and port for next test case
		os.Setenv("HOST", "")
		os.Setenv("PORT", "")
		os.Setenv("GRACEFUL", "")
		os.Setenv("TIMEOUT", "")
		os.Setenv("RECIEVER", "")
		os.Setenv("EMITTER", "")
		os.Setenv("TEMP", "")
		os.Setenv("BUFFER", "")
	}
}
