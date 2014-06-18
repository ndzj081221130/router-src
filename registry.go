package router

import (
	"encoding/json"
	"fmt"
	steno "github.com/cloudfoundry/gosteno"
	"math/rand"
	"router/config"
	"router/stats"
	"router/util"
	"strings"
	"sync"
	"time"
)

type Uri string
type Uris []Uri

type RootTx string
type RootTxs []RootTx

func (u Uri) ToLower() Uri {
	return Uri(strings.ToLower(string(u)))
}

func (ms Uris) Sub(ns Uris) Uris {
	var rs Uris

	for _, m := range ms {
		found := false
		for _, n := range ns {
			if m == n {
				found = true
				break
			}
		}

		if !found {
			rs = append(rs, m)
		}
	}

	return rs
}

func (x Uris) Has(y Uri) bool {
	for _, xb := range x {
		if xb == y {
			return true
		}
	}

	return false
}

func (x Uris) Remove(y Uri) (Uris, bool) {
	for i, xb := range x {
		if xb == y {
			x[i] = x[len(x)-1]
			x = x[:len(x)-1]
			return x, true
		}
	}

	return x, false
}

type BackendId string

type Backend struct {
	sync.Mutex

	*steno.Logger

	BackendId BackendId

	ApplicationId     string
	Host              string
	Port              uint16
	Tags              map[string]string
	PrivateInstanceId string

	Blocked			  bool
	Stats			  string
	U 				  Uris
	Roots 			  RootTxs
	CollectPort		  string
	t time.Time
}

func (b *Backend) MarshalJSON() ([]byte, error) {
	return json.Marshal(b.CanonicalAddr())
}

func newBackend(i BackendId, m *registryMessage, l *steno.Logger) *Backend {
	b := &Backend{
		Logger: l,

		BackendId: i,

		ApplicationId:     m.App,
		Host:              m.Host,
		Port:              m.Port,
		Tags:              m.Tags,
		PrivateInstanceId: m.PrivateInstanceId,
		Blocked:	false,
		U: make([]Uri, 0),
		t: time.Now(),
	}

	return b
}

func (b *Backend) CanonicalAddr() string {
	return fmt.Sprintf("%s:%d", b.Host, b.Port)
}

func (b *Backend) ToLogData() interface{} {
	return struct {
		ApplicationId string
		Host          string
		Port          uint16
		Tags          map[string]string
		Blocked		  bool
	}{
		b.ApplicationId,
		b.Host,
		b.Port,
		b.Tags,
		b.Blocked,
	}
}

func (b *Backend) register(u Uri) bool {
	if !b.U.Has(u) {
		b.Infof("Register %s (%s)", u, b.BackendId)
		b.U = append(b.U, u)
		return true
	}

	return false
}

func (b *Backend) unregister(u Uri) bool {
	x, ok := b.U.Remove(u)
	if ok {
		b.Infof("Unregister %s (%s)", u, b.BackendId)
		b.U = x
	}

	return ok
}

func (b* Backend) setBlocked(f bool) bool{

	b.Blocked = f
	return true
}
func (b* Backend) setStats(m string) bool{

	b.Stats = m
	return true
}
func (b* Backend) setRoots(roots RootTxs) bool{

	b.Roots = roots
	return true
}

func (b* Backend) setCollectPort(port string) bool{

	b.CollectPort = port
	return true
}

// This is a transient struct. It doesn't maintain state.
type registryMessage struct {
	Host string            `json:"host"`
	Port uint16            `json:"port"`
	Uris Uris              `json:"uris"`
	Roots RootTxs		   `json:"roots"`
	Tags map[string]string `json:"tags"`
	App  string            `json:"app"`
    Stats string           `json:"stats"`
	PrivateInstanceId string `json:"private_instance_id"`
	CollectPort		string	   `json:"collect_port"`
}

func (m registryMessage) BackendId() (b BackendId, ok bool) {
	if m.Host != "" && m.Port != 0 {
		b = BackendId(fmt.Sprintf("%s:%d", m.Host, m.Port))
		ok = true
	}

	return
}

type Registry struct {
	sync.RWMutex

	*steno.Logger

	*stats.ActiveApps
	*stats.TopApps

	byUri       map[Uri][]*Backend
	byBackendId map[BackendId]*Backend

	staleTracker *util.ListMap

	pruneStaleDropletsInterval time.Duration
	dropletStaleThreshold      time.Duration

	isStateStale func() bool
}

func NewRegistry(c *config.Config) *Registry {
	r := &Registry{}

	r.Logger = steno.NewLogger("router.registry")

	r.ActiveApps = stats.NewActiveApps()
	r.TopApps = stats.NewTopApps()

	r.byUri = make(map[Uri][]*Backend)
	r.byBackendId = make(map[BackendId]*Backend)

	r.staleTracker = util.NewListMap()

	r.pruneStaleDropletsInterval = c.PruneStaleDropletsInterval
	r.dropletStaleThreshold = c.DropletStaleThreshold

	r.isStateStale = func() bool { return false }

	go r.checkAndPrune()

	return r
}

func (r *Registry) NumUris() int {
	r.RLock()
	defer r.RUnlock()

	return len(r.byUri)
}

func (r *Registry) NumBackends() int {
	r.RLock()
	defer r.RUnlock()

	return len(r.byBackendId)
}

func (r *Registry) registerUri(b *Backend, u Uri) {
	u = u.ToLower()

	ok := b.register(u)
	if ok {
		x := r.byUri[u]
		r.byUri[u] = append(x, b)
	}
}

func (r *Registry) Register(m *registryMessage) {
	i, ok := m.BackendId()
	if !ok || len(m.Uris) == 0 {
		return
	}

	r.Lock()
	defer r.Unlock()

	b, ok := r.byBackendId[i]
	if !ok {
		b = newBackend(i, m, r.Logger)
		r.byBackendId[i] = b
	}
	var s string = m.Stats
	if (s == "NORMAL") || (s == "VALID") || (s == "FREE") {
 		//fmt.Printf("%v , %s, blocked false\n   ", b.BackendId , s)
		b.setBlocked(false)
	}else if(s == "ONDEMAND"  ){
 		//fmt.Printf("%v, %s , blocked true\n", b.BackendId , s)
		b.setBlocked(true)
	}
	
	b.setStats(s)
	
	var roots RootTxs = m.Roots
	b.setRoots(roots)
	
	
	var collect_port = m.CollectPort
	b.setCollectPort(collect_port)
	
	for _, u := range m.Uris {
		r.registerUri(b, u)
	}

	b.t = time.Now()

	r.staleTracker.PushBack(b)
}

func (r *Registry) unregisterUri(b *Backend, u Uri) {
	u = u.ToLower()

	ok := b.unregister(u)
	if ok {
		x := r.byUri[u]
		for i, y := range x {
			if y == b {
				x[i] = x[len(x)-1]
				x = x[:len(x)-1]
				break
			}
		}

		if len(x) == 0 {
			delete(r.byUri, u)
		} else {
			r.byUri[u] = x
		}
	}

	// Remove backend if it no longer has uris
	if len(b.U) == 0 {
		delete(r.byBackendId, b.BackendId)
		r.staleTracker.Delete(b)
	}
}

func (r *Registry) Unregister(m *registryMessage) {
	i, ok := m.BackendId()
	if !ok {
		return
	}

	r.Lock()
	defer r.Unlock()

	b, ok := r.byBackendId[i]
	if !ok {
		return
	}

	for _, u := range m.Uris {
		r.unregisterUri(b, u)
	}
}

func (r *Registry) pruneStaleDroplets() {
	if r.isStateStale() {
		r.resetTracker()
		return
	}

	for r.staleTracker.Len() > 0 {
		b := r.staleTracker.Front().(*Backend)
		if b.t.Add(r.dropletStaleThreshold).After(time.Now()) {
			break
		}

		log.Infof("Pruning stale droplet: %v ", b.BackendId)

		for _, u := range b.U {
			r.unregisterUri(b, u)
		}
	}
}

func (r *Registry) resetTracker() {
	for r.staleTracker.Len() > 0 {
		r.staleTracker.Delete(r.staleTracker.Front().(*Backend))
	}
}

func (r *Registry) PruneStaleDroplets() {
	r.Lock()
	defer r.Unlock()

	r.pruneStaleDroplets()
}

func (r *Registry) checkAndPrune() {
	if r.pruneStaleDropletsInterval == 0 {
		return
	}

	tick := time.Tick(r.pruneStaleDropletsInterval)
	for {
		select {
		case <-tick:
			//log.Debug("Start to check and prune stale droplets")
			r.PruneStaleDroplets()
		}
	}
}

func (r *Registry) Lookup(host string,rootTx string) (*Backend, bool) {
	r.RLock()
	defer r.RUnlock()

	x, ok := r.byUri[Uri(host).ToLower()]
	if !ok {
		return nil, false
	}

	// Return random backend from slice of backends for the specified uri
	//	log.Infof("  droplet: %v ", b.BackendId)
	b := x[rand.Intn(len(x))] // this is random
 	log.Infof("random b: %v ", b.BackendId)
	oldFlag := false
	old := b.Roots
	if old != nil && len(old) >0 {
		for i :=0 ; i < len(old) ;i ++ {
			if string(old[i]) == rootTx {//如果当前的事物id，是old事物，那么用旧版本的应用响应？
				oldFlag = true
				break
			}
		}		
	}
	
	if oldFlag {
 		log.Info("Caution:oldFlag = true")
		if len(x) > 0 {
			b = x[0]// first len(x) >0 , return new version ?
			log.Infof("we use x[0] as oldVersion b: %v ", b.BackendId)
		}
	}
//	else{
//		log.Info("oldFlag = false")//這個是正常情況，因爲roots是空的。應該隨便返回一個啊，爲啥強制返回最後一個？
//		if len(x) > 0 {
//			b = x[len(x) -1]// first len(x) >0 , return new version ?返回最後一個？
//			log.Infof("new b: %v ", b.BackendId)
//		}
//			 
//	}
	
	stats := b.Stats
	
	for ;stats == "ONDEMAND";{
		b = x[rand.Intn(len(x))]
		stats = b.Stats
		log.Info("in for ondemand")
	}
	 
	 
 	return b,true
}

func (r *Registry) LookupByPrivateInstanceId(host string, p string,rootTx string) (*Backend, bool) {
	r.RLock()
	defer r.RUnlock()

	x, ok := r.byUri[Uri(host).ToLower()]
	if !ok {
		return nil, false
	}

	for _, b := range x {
		if b.PrivateInstanceId == p {// come from the same browser?
 			 

			if b.Stats == "ONDEMAND"{
				log.Info("privateInstanceid, status = ondemand , block request")//
				return nil,false
			}else{
				return b,true
			}
		}
	}

	return nil, false
}

func (r *Registry) CaptureBackendRequest(x *Backend, t time.Time) {
	if x.ApplicationId != "" {
		r.ActiveApps.Mark(x.ApplicationId, t)
		r.TopApps.Mark(x.ApplicationId, t)
	}
}

func (r *Registry) MarshalJSON() ([]byte, error) {
	r.RLock()
	defer r.RUnlock()

	return json.Marshal(r.byUri)
}
