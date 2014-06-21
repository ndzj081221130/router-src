package router

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"fmt"
	nats "github.com/cloudfoundry/gonats"
	steno "github.com/cloudfoundry/gosteno"
	"net"
	vcap "router/common"
	"router/config"
	"router/proxy"
	"router/util"
	"runtime"
	"time"
	"errors"
	"io"
)

const RECV_BUF_LEN = 1024

type Router struct {
	config     *config.Config
	proxy      *Proxy
	natsClient *nats.Client
	registry   *Registry
	varz       Varz
}

func (r *Router) StartTxServer() {
    listener, err := net.Listen("tcp", "0.0.0.0:6666")//侦听在6666端口
    if err != nil {
        panic("error listening:"+err.Error())
    }
    fmt.Println("Starting the tx server")
          
    for{
    	//runtime.Gosched()
        conn, err := listener.Accept() //接受连接
        if err != nil {
            panic("Error accept:"+err.Error())
        }
        fmt.Println("Accepted the Connection :", conn.RemoteAddr())
        go EchoServer(conn,r.proxy)
    }
}

func EchoServer(conn net.Conn , proxy *Proxy) {
    buf := make([]byte, RECV_BUF_LEN)
    defer conn.Close()
          
    for{
        n, err := conn.Read(buf);
        switch err {
            case nil:
            	msg := string(buf[0:n])
//            	fmt.Printf("echo server : %s \n" ,msg)
            	 
				var tm TxMessage

				err := json.Unmarshal(buf[0:n], &tm)
				if err != nil {
					fmt.Printf("unmarshal error \n")
					lm := fmt.Sprintf("---%s: Error unmarshalling JSON: %s", "txMessage", err)
					log.Log(steno.LOG_WARN, lm, map[string]interface{}{"payload": string(msg)})
					continue
				}
				remote := proxy.RootTxExist(tm.InstanceId)
				if remote == nil {
//					fmt.Printf("remote nil \n ")
                	conn.Write( buf[0:n] )
                	return
                }else {
//                	fmt.Printf("hahaha, has tx for %s \n" , tm.InstanceId)
                	new_b,marshal_err := json.Marshal(remote)
                	if marshal_err != nil {
                		lm := fmt.Sprintf("---%s: Error unmarshalling JSON: %s", "txMessage", err)
						log.Log(steno.LOG_WARN, lm, map[string]interface{}{"payload": string(msg)})
//						fmt.Printf("marshal error ?\n")
						conn.Write( buf[0:n] )
						return
                	}else{
//                		fmt.Printf("marshal succ %s \n" , string(new_b))
                		conn.Write(new_b)
                		return
                	}
                }
            case io.EOF:
                fmt.Printf("Warning: End of data: %s \n", err);
                return
            default:
                fmt.Printf("Error: Reading data : %s \n", err);
                return
        }
     }
}

func NewRouter(c *config.Config) *Router {
	r := &Router{
		config: c,
	}

	// setup number of procs
	if r.config.GoMaxProcs != 0 {
		runtime.GOMAXPROCS(r.config.GoMaxProcs)
	}

	// setup nats
	r.establishNATS()
	
	r.registry = NewRegistry(r.config)
	r.registry.isStateStale = func() bool {
		return !r.natsClient.Ping()
	}

	r.varz = NewVarz(r.registry)
	r.proxy = NewProxy(r.config, r.registry, r.varz,r)
//怎么把自己也传过去，作为参数
	varz := &vcap.Varz{
		UniqueVarz: r.varz,
	}
	
	healthz := &vcap.Healthz{
	  LockableObject: r.registry,
	}

	var host string
	if r.config.Status.Port != 0 {
		host = fmt.Sprintf("%s:%d", r.config.Ip, r.config.Status.Port)
	}

	component := &vcap.VcapComponent{
		Type:        "Router",
		Index:       r.config.Index,
		Host:        host,
		Credentials: []string{r.config.Status.User, r.config.Status.Pass},
		Config:      r.config,
		Logger:      log,
		Varz:        varz,
		Healthz:     healthz,
		InfoRoutes: map[string]json.Marshaler{
			"/routes": r.registry,
		},
	}

	vcap.Register(component, r.natsClient)
	
	return r
}

func (r *Router) subscribeRegistry(subject string, fn func(*registryMessage)) {
	s := r.natsClient.NewSubscription(subject)
	s.Subscribe()

	go func() {
		for m := range s.Inbox {
			var rm registryMessage

			err := json.Unmarshal(m.Payload, &rm)
			if err != nil {
				lm := fmt.Sprintf("---%s: Error unmarshalling JSON: %s", subject, err)
				log.Log(steno.LOG_WARN, lm, map[string]interface{}{"payload": string(m.Payload)})
				continue
			}

			lm := fmt.Sprintf("zhang:%s: Received message", subject)
			log.Log(steno.LOG_DEBUG, lm, map[string]interface{}{"message": rm})

			fn(&rm)
		}
	}()
}

func (r *Router) SubscribeRegister() {
	r.subscribeRegistry("router.register", func(rm *registryMessage) {
		r.registry.Register(rm)
	})
}


func (r *Router) SubscribeStatus() {
//	r.subscribeRegistry("router.status", func(rm *registryMessage) {
////		r.registry.Register(rm)
//	})
}


func (r *Router) SubscribeUnregister() {
	r.subscribeRegistry("router.unregister", func(rm *registryMessage) {
		r.registry.Unregister(rm)
	})
}

func (r *Router) flushApps(t time.Time) {
	x := r.registry.ActiveSince(t)

	y, err := json.Marshal(x)
	if err != nil {
		log.Warnf("flushApps: Error marshalling JSON: %s", err)
		return
	}

	b := bytes.Buffer{}
	w := zlib.NewWriter(&b)
	w.Write(y)
	w.Close()

	z := b.Bytes()

	log.Debugf("Active apps: %d, message size: %d", len(x), len(z))

	r.natsClient.Publish("router.active_apps", z)
}

func (r *Router) ScheduleFlushApps() {
	if r.config.PublishActiveAppsInterval == 0 {
		return
	}

	go func() {
		t := time.NewTicker(r.config.PublishActiveAppsInterval)
		x := time.Now()

		for {
			select {
			case <-t.C:
				y := time.Now()
				r.flushApps(x)
				x = y
			}
		}
	}()
}

func (r *Router) SendStartMessage() {
	host, err := vcap.LocalIP()
	if err != nil {
		panic(err)
	}
	d := vcap.RouterStart{vcap.GenerateUUID(), []string{host}}

	b, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}

	// Send start message once at start
	r.natsClient.Publish("router.start", b)

	go func() {
		t := time.NewTicker(r.config.PublishStartMessageInterval)

		for {
			select {
			case <-t.C:
				r.natsClient.Publish("router.start", b)
			}
		}
	}()
}


//type Param string
//type Params []Param

type remoteMessage struct {
	RootTx string        		`json:"RootTx"`
	ParentTx string             `json:"ParentTx"`
	ParentPort string			`json:"ParentPort"`
	ParentName string			`json:"ParentName"`
 	SubPort 	string			`json:"SubPort"`
	SubName		string			`json:"SubName"` 
	InvocationCtx string		`json:"InvocationCtx"`
}

type TxMessage struct {
	InstanceId string 		`json:"InstanceId"`
	
}


 

func (r *Router) Run() {
	var err error

	// Subscribe register/unregister router
	r.SubscribeRegister()
	r.SubscribeUnregister()
	r.SubscribeStatus()
	// Kickstart sending start messages
	r.SendStartMessage()

	// Schedule flushing active app's app_id
	r.ScheduleFlushApps()
	fmt.Printf("available cpu =  %d \n" , runtime.NumCPU() )
	//runtime.GOMAXPROCS(2)
	
	// Wait for one start message send interval, such that the router's registry
	// can be populated before serving requests.
	if r.config.PublishStartMessageInterval != 0 {
		log.Infof("Waiting %s before listening...", r.config.PublishStartMessageInterval)
		time.Sleep(r.config.PublishStartMessageInterval)
	}
	fmt.Printf(":%d \n", r.config.Port)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", r.config.Port))
	if err != nil {
		log.Fatalf("net.Listen: %s", err)
	}

	util.WritePidFile(r.config.Pidfile)
	
	log.Infof("Listening on %s", l.Addr())

	s := proxy.Server{Handler: r.proxy} // 这里的proxy？ Server？？

	err = s.Serve(l)//这里估计也是一个死循环？
	if err != nil {
		log.Fatalf("proxy.Serve: %s", err)
	}
	
	
}

func (r *Router) establishNATS() {
	r.natsClient = nats.NewClient()

	host := r.config.Nats.Host
	user := r.config.Nats.User
	pass := r.config.Nats.Pass

	go func() {
		for {
			e := r.natsClient.RunWithDefaults(host, user, pass)
//			 
			 
			err := errors.New(e.Error() + " is a test")
			log.Warnf("Failed to connect to nats server: %s host:%s,user:%s,pass:%s ", err.Error(),host,user,pass)

			time.Sleep(1 * time.Second)
			
			r.natsClient = nats.NewClient()
		}
	}()
}
