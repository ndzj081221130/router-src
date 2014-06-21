package router

import (
	"bufio"
	"fmt"
	steno "github.com/cloudfoundry/gosteno"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"router/config"
	"strings"
	"sync"
	"time"
	"encoding/json"
 )

const (
	VcapBackendHeader = "X-Vcap-Backend"
	VcapRouterHeader  = "X-Vcap-Router"
	VcapTraceHeader   = "X-Vcap-Trace"

	VcapCookieId    = "__VCAP_ID__"
	StickyCookieKey = "JSESSIONID"
)

type Proxy struct {//为啥不予要router对象，可以传消息啊。。。
	sync.RWMutex
	*steno.Logger
	*config.Config
	*Registry
	Varz
	*AccessLogger
	*Router//传对象。还是指针？传指针好了
	
	CachedTxBackends    map[string]*Backend      // map[string]string
	
	CachedBackendTxes   map[string][]*remoteMessage //到底时以什么为key，instance （string） ? 还是 Backend?
}

type responseWriter struct {
	http.ResponseWriter
	*steno.Logger
}
// DEFINED by zhangjie 
var zlog *steno.Logger

func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj := rw.ResponseWriter.(http.Hijacker)
	return hj.Hijack()
}

func (rw *responseWriter) WriteStatus(code int) {
	body := fmt.Sprintf("%d %s", code, http.StatusText(code))
	rw.Warn(body)
	http.Error(rw, body, code)
}

func (rw *responseWriter) CopyFrom(src io.Reader) (int64, error) {
	if src == nil {
		return 0, nil
	}

	var dst io.Writer = rw

	// Use MaxLatencyFlusher if needed
	if v, ok := rw.ResponseWriter.(writeFlusher); ok {
		u := NewMaxLatencyWriter(v, 50*time.Millisecond)
		defer u.Stop()
		dst = u
	}

	return io.Copy(dst, src)
}

func NewProxy(c *config.Config, r *Registry, v Varz,router  *Router) *Proxy {
	p := &Proxy{
		Config:   c,
		Logger:   steno.NewLogger("router.proxy"),
		Registry: r,
		Varz:     v,
		Router:   router, // this is add by me
	}

	p.CachedTxBackends = map[string]*Backend{}
	p.CachedBackendTxes = map[string][]*remoteMessage{} 
	if c.AccessLog != "" {
		f, err := os.OpenFile(c.AccessLog, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			panic(err)
		}

		p.AccessLogger = NewAccessLogger(f)
		go p.AccessLogger.Run()
	}

	return p
}

func hostWithoutPort(req *http.Request) string {
	host := req.Host

	// Remove :<port>
	pos := strings.Index(host, ":")
	if pos >= 0 {
		host = host[0:pos]
	}

	return host
}

func (p *Proxy) Lookup(req *http.Request,parentTx string) (*Backend, bool) {
	h := hostWithoutPort(req)

	// Try choosing a backend using sticky session
	sticky, err := req.Cookie(VcapCookieId)// private_instance_id
	//p.Infof("in Lookup %s " , sticky)
	if err == nil {
		b, ok := p.Registry.LookupByPrivateInstanceId(h, sticky.Value,parentTx)
		if ok {
			p.Infof("get stick value! %s " , b)
			return b, ok
		}
	}

	// Choose backend using host alone
	return p.Registry.Lookup(h,parentTx)
} 

type Param string
type Params []Param

type reqMessage struct {
	Method string        `json:"method"`
	ID string            `json:"id"`
	Params Params       `json:"params"`
	 
}
//umarshal error json: cannot unmarshal object key \"params\" into 
//unexported field mParams of type router.reqMessage 
// {\"method\":\"sayHello\",\"id\":\"b8070565-1d61-4857-9f8f-cfb966d4df8c\",
//\"params\":[\"World|parentTx=2c2914e4-8a28-4b15-b7e2-29ff570ac47f\"]}"
 
// the HTTP request is from dea push cmd, from 10.0.2.15:5678
func (p *Proxy) ServeHTTP(hrw http.ResponseWriter, req *http.Request) {
	rw := responseWriter{
		ResponseWriter: hrw,
		Logger:         p.Logger.Copy(),
	}
	// log for zjlog!
	stenoConfig := &steno.Config{
		Sinks: []steno.Sink{steno.NewIOSink(os.Stderr)},
		Codec: steno.NewJsonCodec(),
		Level: steno.LOG_ALL,
	}

	steno.Init(stenoConfig)
	zlog = steno.NewLogger("router.test")
	 
 
	rw.Set("RemoteAddr", req.RemoteAddr)
	rw.Set("Host", req.Host)
	rw.Set("X-Forwarded-For", req.Header["X-Forwarded-For"]) //  
 	rw.Set("X-Forwarded-Proto", req.Header["X-Forwarded-Proto"])
 	a := AccessLogRecord{
		Request:   req,
		StartedAt: time.Now(),
	}

	if req.ProtoMajor != 1 && (req.ProtoMinor != 0 || req.ProtoMinor != 1) {
		c, brw, err := rw.Hijack()
		if err != nil {
			panic(err)
		}

		fmt.Fprintf(brw, "HTTP/1.0 400 Bad Request\r\n\r\n")
		brw.Flush()
		c.Close()
		return
	}

	start := time.Now()

	// Return 200 OK for heartbeats from LB
	if req.UserAgent() == "HTTP-Monitor/1.1" {
		rw.WriteHeader(http.StatusOK)
		fmt.Fprintln(rw, "ok")
		return
	}
	
	rw.Infof("zhang:req= %s",req)
  	bytes,_  := ioutil.ReadAll(req.Body)
 

	parentTx := ""
	rootTx := ""
	parentName := ""
	parent_port := ""
	subName := ""
	invocationCtx := ""
	cat  := string(bytes) 
	var rm reqMessage 
	u_err := json.Unmarshal(bytes, &rm)
	if u_err != nil{
		rw.Infof("cat= %s",cat)
		rw.Infof("!umarshal error %s " , u_err)
	}
	if len(rm.Params) > 0 {
			str := string(rm.Params[0])//居然index out of range
			rw.Info(str)
			 
		     arr := strings.Split(str,"|")
//		     rw.Infof("0=%s", arr[0])
//		     rw.Infof("1=%s",arr[1])
//		     
		     rootTx = arr[1]
		     parentTx = arr[2]
		     
		     if len(arr) > 6{
		     	parentName = arr[3] // actually , this is current component
		     	parent_port = arr[4]
		     	subName = arr[5]
		     	invocationCtx = arr[6]
		     }
		     rm.Params[0] = Param(arr[0])//什么意思？？？
			 y, err := json.Marshal(rm)
			 if err != nil {
			 	rw.Infof("marshal error %s ",err)
			 }
		     req.Body = ioutil.NopCloser(strings.NewReader(string(y)) )// 
	 
	}else{
		rw.Infof("rm.Params == nil %s", rm.Params )
	}
	
	//
	rw.Infof("mybody = %s" , cat) // bufio.Reader
	//所以，或许parentTx要在lookup前  

	//首先时获取backend，如果此处因为状态，返回nil，那么，就不会再有请求了。
	x, ok := p.Lookup(req,rootTx)//   get backend,  if there is JSESSIONID , router to the same backend
	if !ok {
		p.Varz.CaptureBadRequest(req)
		rw.WriteStatus(http.StatusNotFound)
		return
	}
		fmt.Printf("check whether use cachedTxBackends \n")
	if rootTx != "" {
	
		h := hostWithoutPort(req)
		fmt.Printf("h = %s \n" , h)
	 	find_key := x.ApplicationId + ":" + rootTx
	 	if p.CachedTxBackends[find_key] != nil {
	 		x = p.CachedTxBackends[find_key]
	 		fmt.Printf("%s get key:%s from map ,tx = %s\n" ,x.CollectPort, x.ApplicationId, rootTx)
	 	}else{
			key := x.ApplicationId + ":" + rootTx
			p.CachedTxBackends[key] = x 
			fmt.Printf( "%s save key:%s to map ,tx = %s  \n",x.CollectPort,x.ApplicationId , rootTx)
		}

		fmt.Printf("%v ,%d, %s, blocked false\n   ", x.BackendId ,x.ApplicationId, x.Port)
		sub_port := x.CollectPort
		
		remote := &remoteMessage{
			RootTx: rootTx,
			ParentTx: parentTx,
			ParentPort: parent_port,
			ParentName: parentName,
			SubPort: 	sub_port,
			SubName:	subName,
			InvocationCtx:	invocationCtx,
		} 
		
		cachedTxes := p.CachedBackendTxes[x.PrivateInstanceId]  
		p.CachedBackendTxes[x.PrivateInstanceId] = append(cachedTxes , remote)
		
//		fmt.Printf("after add \n")
//		 
//			
//		for  x,y := range p.CachedBackendTxes {
//			for j := 0 ; j < len (y); j++ {
//				fmt.Printf("i:%s,j:%d,remote:%s.\n",x,j,y[j].RootTx)
//			}
//		}
	}else{
	
		fmt.Printf("rootTx == whitespace \n")
	} 
	
	rw.Set("Backend", x.ToLogData(	))

	a.Backend = x

	p.Registry.CaptureBackendRequest(x, start)//record request
	p.Varz.CaptureBackendRequest(x, req)

	req.URL.Scheme = "http"
	req.URL.Host = x.CanonicalAddr()

	// Add X-Forwarded-For
	if host, _, err := net.SplitHostPort(req.RemoteAddr); err == nil {
		// We assume there is a trusted upstream (L7 LB) that properly
		// strips client's XFF header

		// This is sloppy but fine since we don't share this request or
		// headers. Otherwise we should copy the underlying header and
		// append
		xff := append(req.Header["X-Forwarded-For"], host)
		req.Header.Set("X-Forwarded-For", strings.Join(xff, ", "))
	}

	// Check if the connection is going to be upgraded to a WebSocket connection
	if p.CheckWebSocket(rw, req) {
		p.ServeWebSocket(rw, req)
		return
	}

	// Use a new connection for every request
	// Keep-alive can be bolted on later, if we want to
	req.Close = true
	req.Header.Del("Connection")
	
    // this is used for send request and get resposne
	
	res, err := http.DefaultTransport.RoundTrip(req)

	latency := time.Since(start)

	a.FirstByteAt = time.Now()
	a.Response = res

	if err != nil {
		p.Varz.CaptureBackendResponse(x, res, latency)
		rw.Warnf("Error reading from upstream: %s", err)
		rw.WriteStatus(http.StatusBadGateway)
		return
	}
	
	rw.Infof("zhang:res= %s",res)
	p.Varz.CaptureBackendResponse(x, res, latency)

	for k, vv := range res.Header {
		for _, v := range vv {
			rw.Header().Add(k, v)
		}
	}

	if p.Config.TraceKey != "" && req.Header.Get(VcapTraceHeader) == p.Config.TraceKey {
		rw.Header().Set(VcapRouterHeader, p.Config.Ip)
		rw.Header().Set(VcapBackendHeader, x.CanonicalAddr())
	}

	needSticky := false
	 
	//这里是判断response中，cookie中是否有jsessionid
	for _, v := range res.Cookies() {// here we get res.Cookies(), if exists v[i].name = StickyCookieKey
		zlog.Info(v.Name)//基于Cookie的web应用，会将数据存成<key,value>
		//但是基于Http Session的应用，为什么在这里只看到了jsessionid？
		
		if v.Name == StickyCookieKey {//JSESSIONID
			needSticky = true // here flag = needSticky
			break
		}
	}
	 
	 
	if needSticky && x.PrivateInstanceId != "" {
		cookie := &http.Cookie{ // here we got http cookie which has sessionid
			Name:  VcapCookieId,
			Value: x.PrivateInstanceId,
			Path:  "/",
		} //
		 
		http.SetCookie(rw, cookie) // set httpCookie 只在发现有sticky session时，才set Cookie
		// http session,第一次生成jsessionid时，会在返回头部加上cookie
	}

	rw.WriteHeader(res.StatusCode)
	n, _ := rw.CopyFrom(res.Body)

	a.FinishedAt = time.Now()
	a.BodyBytesSent = n

	if p.AccessLogger != nil {
		p.AccessLogger.Log(a)
	}
}

func (p * Proxy) RootTxExist(instance_id  string) *remoteMessage {
	remotes := p.CachedBackendTxes[instance_id]
	if remotes == nil {
		fmt.Printf("no cached tx for %s \n" , instance_id)
		return nil
	}else{
		r := remotes[0]
		
		if len(remotes) == 1 {
			fmt.Printf("only one cached tx for %s \n" , instance_id)
			delete(p.CachedBackendTxes , instance_id)	
		}else{
			fmt.Printf("more than one cached tx for %s \n", instance_id)
			p.CachedBackendTxes[instance_id] = remotes[1:len(remotes)] 
		}
		
//		fmt.Printf("after delete \n")
//		 
//			
//		for  x,y := range p.CachedBackendTxes {
//			for j := 0 ; j < len (y); j++ {
//				fmt.Printf("i:%s,j:%d,remote:%s.\n",x,j,y[j].RootTx)
//			}
//		}
		
		
		return r
	}
	
	return nil
}


func (p *Proxy) CheckWebSocket(rw http.ResponseWriter, req *http.Request) bool {
	return req.Header.Get("Connection") == "Upgrade" && req.Header.Get("Upgrade") == "websocket"
}

func (p *Proxy) ServeWebSocket(rw responseWriter, req *http.Request) {
	var err error

	rw.Set("Upgrade", "websocket")

	dc, _, err := rw.Hijack()
	if err != nil {
		rw.Warnf("hj.Hijack: %s", err)
		rw.WriteStatus(http.StatusBadRequest)
		return
	}

	defer dc.Close()

	// Dial backend
	uc, err := net.Dial("tcp", req.URL.Host)
	if err != nil {
		rw.Warnf("net.Dial: %s", err)
		rw.WriteStatus(http.StatusBadRequest)
		return
	}

	defer uc.Close()

	// Write request
	err = req.Write(uc)
	if err != nil {
		rw.Warnf("Writing request: %s", err)
		rw.WriteStatus(http.StatusBadRequest)
		return
	}

	errch := make(chan error, 2)

	copy := func(dst io.Writer, src io.Reader) {
		_, err := io.Copy(dst, src)
		if err != nil {
			errch <- err
		}
	}

	go copy(uc, dc)
	go copy(dc, uc)

	// Don't care about error, both connections will be closed if necessary
	<-errch
}
