package main

import (
	"flag"
	"router"
	"router/config"
	"runtime"
)

var configFile string

func init() {
	flag.StringVar(&configFile, "c", "", "Configuration File")

	flag.Parse()
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())
	c := config.DefaultConfig()
	if configFile != "" {
		c = config.InitConfigFromFile(configFile)
	}

	router.SetupLoggerFromConfig(c)
	
	r := router.NewRouter(c)
	
//	go r.S
	 
	go r.Run()
	
	r.StartTxServer()
}
