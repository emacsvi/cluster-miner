package cluster

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/clientv3"
	"gopkg.in/urfave/cli.v2"
)

type SectorStorageInfo struct {
	ID     string
	URLs   []string // TODO: Support non-http transports
	Weight uint64

	CanSeal  bool
	CanStore bool
}

type Meta struct {
	IsSeal      bool
	NFSSealDir  string
	Srv         string
	ActorId     string
	RepoStorage string
	Hostname    string
	cli         *SingleConnRpcClient
	actor       string

	// etcdv3
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher

	// master会监视所有Add请求
	sync.RWMutex
	M map[uint64]*SectorStorageInfo

	// seal 会put所有Sector信息
}

func (m *Meta) String() string {
	return fmt.Sprintf("<Srv:%s, ActorId:%s, RepoStorage:%s, Hostname:%s, NFSSealDir:%s>",
		m.Srv, m.ActorId, m.RepoStorage, m.Hostname, m.NFSSealDir)
}

var (
	SectorsMeta *Meta
)

func InitMetaFlag(cli *cli.Context, actor string) (err error) {
	var (
		isSeal   bool
		nfs      string
		srv      string
		repo     string
		hostname string

		// etcdv3
		endpoints []string
		client    *clientv3.Client
		kv        clientv3.KV
		lease     clientv3.Lease
		watcher   clientv3.Watcher
	)

	isSeal = cli.Bool("seal")

	nfs = cli.String("nfs")
	srv = cli.String("srv")
	repo = cli.String("storagerepo")
	repo = getBase(repo)

	hostname, err = Hostname()
	if err != nil {
		log.Fatalln("hostname must seted.", err)
		return
	}

	endpoints = cli.StringSlice("cluster")
	if len(endpoints) == 0 {
		err = errors.New("cluster is empty.")
		return
	}

	if client, err = NewClient(endpoints); err != nil {
		return
	}

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	SectorsMeta = &Meta{
		IsSeal:      isSeal,
		NFSSealDir:  nfs,
		Srv:         srv,
		RepoStorage: repo,
		ActorId:     actor,
		Hostname:    hostname,
		actor:       actor,

		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,

		M: make(map[uint64]*SectorStorageInfo),
	}

	// 只有master才开启此功能
	if !isSeal {
		log.Println("do loopWatch meta just for master.")
		err = SectorsMeta.loopWatch()
	}
	return
}

// seal 节点只做Save
func (m *Meta) Save(id uint64, v SectorStorageInfo) (err error) {
	var (
		key     string
		value   string
		putResp *clientv3.PutResponse
	)
	key = SECTORS_META_PREFIX + fmt.Sprintf("%s/%d", m.actor, id)
	value, err = encodeStorageInfo(&v)
	if err != nil {
		return err
	}
	if putResp, err = m.kv.Put(context.TODO(), key, value, clientv3.WithPrevKV()); err != nil {
		log.Printf("error save to etcd [%s]=%v\n", key, v)
		return
	}

	log.Printf("success save to etcd [%s]=%s => %v\n", key, value, v)

	if putResp.PrevKv != nil {
		log.Println("error while put sector meta info:", id, " and k-v: ", string(putResp.PrevKv.Key),
			string(putResp.PrevKv.Value))
	}

	return
}

func (m *Meta) GetMeta(id uint64) (meta SectorStorageInfo, ok bool) {
	var p *SectorStorageInfo
	m.RLock()
	defer m.RUnlock()
	p, ok = m.M[id]
	if ok {
		meta = *p
	}
	return
}

// master 会一直监控事件
func (m *Meta) loopWatch() (err error) {
	var (
		getResp            *clientv3.GetResponse
		kvpair             *mvccpb.KeyValue
		watchChan          clientv3.WatchChan
		watchResp          clientv3.WatchResponse
		watchEvent         *clientv3.Event
		watchStartRevision int64
		mk                 uint64
		watchKey           string
	)

	watchKey = fmt.Sprintf("%s%s/", SECTORS_META_PREFIX, m.actor)
	log.Println("meta loopWatch Key=", watchKey)

	if getResp, err = m.kv.Get(context.TODO(), watchKey, clientv3.WithPrefix()); err != nil {
		return
	}

	for _, kvpair = range getResp.Kvs {
		log.Printf("Add [%s]=%s\n", string(kvpair.Key), string(kvpair.Value))
		mk, err = getIdFromBase(string(kvpair.Key))
		if err != nil || mk == 0 {
			log.Println("Parse key", string(kvpair.Key), " happened error:", err)
			continue
		}
		info, err := decodeStorageInfo(string(kvpair.Value))
		if err != nil {
			log.Println("decode error:", err)
			continue
		}
		log.Printf("Add [%d]=%v\n", mk, info)
		m.M[mk] = &info
	}

	// watch the event from this revision
	go func() {
		watchStartRevision = getResp.Header.Revision + 1
		watchChan = m.watcher.Watch(context.TODO(), watchKey, clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())

		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case clientv3.EventTypePut:
					log.Printf("PUT Event and [%s]=%s\n", string(watchEvent.Kv.Key), string(watchEvent.Kv.Value))
					m.Lock()
					mk, err = getIdFromBase(string(watchEvent.Kv.Key))
					log.Println("mk:", mk)
					if err != nil || mk == 0 {
						log.Println("Parse key", string(kvpair.Key), " happened error:", err)
					} else {
						info, err := decodeStorageInfo(string(watchEvent.Kv.Value))
						if err != nil {
							log.Println("decode error:", err)
						} else {
							log.Printf("success PUT [%d]=%v\n", mk, info)
							m.M[mk] = &info
						}
					}
					m.Unlock()
				case clientv3.EventTypeDelete:
					log.Printf("Delete Event and [%s]=%s\n", string(watchEvent.Kv.Key), string(watchEvent.Kv.Value))
				}
			}
		}
	}()

	log.Println("over init")
	return
}

func encodeStorageInfo(v *SectorStorageInfo) (string, error) {
	out, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(out), nil
}

func decodeStorageInfo(v string) (SectorStorageInfo, error) {
	var s SectorStorageInfo
	data, err := base64.RawURLEncoding.DecodeString(v)

	if err != nil {
		return s, fmt.Errorf("continue key is not valid: %v", err)
	}

	if err := json.Unmarshal(data, &s); err != nil {
		return s, fmt.Errorf("continue key is not valid: %v", err)
	}

	return s, nil
}
