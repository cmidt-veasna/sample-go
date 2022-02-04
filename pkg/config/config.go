// Package config provides the method to read the configation data.
package config

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// A Configuration reresent the configuration data that neccesary start the application.
type Configuration struct {
	Host              string        `env:"HOST" def:"localhost" desc:"IP address or hostname"`   // IP address, domain or hostname for http server
	Port              int           `env:"PORT" def:"8080" desc:"Network port number"`           // A network port number
	GracefulPeriod    time.Duration `env:"GRACEFUL" def:"30" desc:"Graceful shutdown period"`    // default to 30 second
	ConnectionTimeOut time.Duration `env:"TIMEOUT" def:"5" desc:"Timeout for each http request"` // default to 5 second
	RecieveWorker     int           `env:"RECIEVER" def:"10" desc:"Number of worker handle input data"`
	EmitterWorker     int           `env:"EMITTER" def:"5" desc:"Number of worker handle writing data to storage"`
	TemporayFolder    string        `env:"TEMP" def:"data" desc:"Temporary folder to cache input data"`
	BufferSize        ByteSize      `env:"BUFFER" def:"100MB" desc:"Buffer size to read from request body. e.g 4MB"`
}

var conf Configuration
var once sync.Once

// GetConfiguration returns a singleton instance of the Configuration.
// Users can provide configuration data via environment variable or
// command line argument input however if both are present then
// the command line argument is being used.
func GetConfiguration() Configuration {
	once.Do(func() {
		conf = Configuration{}
		conf.init("sample", os.Args[1:])
	})
	return conf
}

// flagMeta store info about struct field and flag
type flagMeta struct {
	field       reflect.Value // index of the field in the struct
	envValue    interface{}   // value from environment variable
	flagValue   interface{}
	hasArgument bool
	transform   func(reflect.Value, interface{})
}

// Init read configuration data from the environment variable
func (c *Configuration) init(name string, args []string) {
	// build flag and read value from environment variable
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flagInfos := make(map[string]*flagMeta)
	configType := reflect.TypeOf(c).Elem()
	configValue := reflect.ValueOf(c).Elem()
	for i := 0; i < configType.NumField(); i++ {
		field := configType.Field(i)
		if field.IsExported() {
			if envName, ok := field.Tag.Lookup("env"); ok {
				fvalue := configValue.Field(i)
				// environment variable usually define with snake case and uppercase letter
				// where flag is usually define with kebab case and lowercase letter
				flagName := strings.ToLower(strings.ReplaceAll(envName, "_", "-"))
				flagInfo := &flagMeta{field: fvalue}
				switch kind := field.Type.Kind(); kind {
				case reflect.String, reflect.Struct:
					if envVal := os.Getenv(envName); envVal != "" {
						flagInfo.envValue = &envVal
					}
					flagInfo.flagValue = flags.String(flagName, field.Tag.Get("def"), field.Tag.Get("desc"))
					switch flagInfo.field.Interface().(type) {
					case ByteSize:
						flagInfo.transform = byteSizeTransformer
					}
				case reflect.Int, reflect.Int64:
					def := field.Tag.Get("def")
					defVal := int64(0)
					if def != "" {
						var err error
						if defVal, err = strconv.ParseInt(def, 10, 64); err != nil {
							panic(fmt.Sprintf("configuration field \"%s\" default value \"%s\" not a valid integer", def, field.Name))
						}
					}
					if envVal := os.Getenv(envName); envVal != "" {
						enviVal, err := strconv.ParseInt(envVal, 10, 64)
						if err != nil {
							panic(fmt.Sprintf("environment variable \"%s\" value \"%s\" is not a valid integer", envName, envVal))
						}
						if kind == reflect.Int {
							iv := int(enviVal)
							flagInfo.envValue = &iv
						} else {
							flagInfo.envValue = &enviVal
						}
					}
					if kind == reflect.Int {
						flagInfo.flagValue = flags.Int(flagName, int(defVal), field.Tag.Get("desc"))
					} else {
						flagInfo.flagValue = flags.Int64(flagName, defVal, field.Tag.Get("desc"))
					}
					switch flagInfo.field.Interface().(type) {
					case time.Duration:
						flagInfo.transform = durationTransformer
					}
				}
				flagInfos[flagName] = flagInfo
			}
		}
	}

	flags.Parse(args)
	flags.Visit(func(f *flag.Flag) {
		flagInfos[f.Name].hasArgument = true
	})

	// command line argument is priority over environment variable
	// however default value will not override environment variable
	for _, fm := range flagInfos {
		var val interface{}
		if !fm.hasArgument && fm.envValue != nil {
			val = fm.envValue
		} else {
			val = fm.flagValue
		}

		if fm.transform != nil {
			fm.transform(fm.field, val)
		} else {
			fm.field.Set(reflect.ValueOf(val).Elem())
		}
	}
}

// durationTransformer convert integer value into `time.Duration` in second.
func durationTransformer(field reflect.Value, value interface{}) {
	field.Set(reflect.ValueOf(time.Duration(*value.(*int64)) * time.Second))
}

// ByteSize present number of byte a data have. It use to convert string suffix like MB, GB into integer
type ByteSize struct {
	raw  string
	Size int64
}

// byteSizeTransformer convert string suffix with MB or GB into integer represent total
// number of byte.
func byteSizeTransformer(field reflect.Value, value interface{}) {
	raw := strings.ToLower(*value.(*string))
	amplify := int64(1)
	switch raw[len(raw)-2:] {
	case "mb":
		amplify = 1024
	case "gb":
		amplify = 1024 * 1024
	case "tb":
		amplify = 1024 * 1024 * 1024
	case "pb":
		amplify = 1024 * 1024 * 1024 * 1024
	}
	// we're in initial pharse to parse configuration
	// if there any error we exit app with error message immediately
	baseVal, err := strconv.ParseInt(raw[:len(raw)-2], 10, 64)
	if err != nil {
		fmt.Printf("invalid byte size %s, it must be suffix with MB or GB ..etc", *value.(*string))
		os.Exit(1)
	}
	field.Set(reflect.ValueOf(ByteSize{
		raw:  *value.(*string),
		Size: baseVal * amplify,
	}))
}
