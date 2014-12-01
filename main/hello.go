package main

import (
	//"bufio"
	"fmt"
	steno "github.com/cloudfoundry/gosteno"
	//"io"
	//"net"
	//"net/http"
	"os"
	//"router/config"
	//"strings"
	//"sync"
	//"time"
)

var log *steno.Logger

 

type Proxy struct {
	*steno.Logger
}
func main(){
	

	stenoConfig := &steno.Config{
		Sinks: []steno.Sink{steno.NewIOSink(os.Stderr)},
		Codec: steno.NewJsonCodec(),
		Level: steno.LOG_ALL,
	}

	steno.Init(stenoConfig)
	log = steno.NewLogger("router.test")
	s := true
	log.Infof("test %s ",s)

p := &Proxy{}
	p.Logger = steno.NewLogger("router.registry")
	p.Infof("hello %s" , "ss\n")
	fmt.Printf("fmt world\n")
}
