package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/clientv3"
	"gopkg.in/urfave/cli.v2"
	"log"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type SectorMgrInfo struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
	nfs     string
	actor   string

	sync.RWMutex
	M map[uint64]string
}

var G_setctorInfo *SectorMgrInfo

// /mnt/nfs/nfs_3970/sealed
func (s *SectorMgrInfo) GetDirPath(id uint64) (path string, ok bool) {
	s.RLock()
	defer s.RUnlock()
	path, ok = s.M[id]
	return
}

func (s *SectorMgrInfo) loopWatch() (err error) {
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

	watchKey = fmt.Sprintf("%s%s/", SECTORS_KEY_PREFIX, s.actor)
	log.Println("watchKey=", watchKey)

	if getResp, err = s.kv.Get(context.TODO(), watchKey, clientv3.WithPrefix()); err != nil {
		return
	}

	for _, kvpair = range getResp.Kvs {
		// log.Printf("Add [%s]=%s\n", string(kvpair.Key), string(kvpair.Value))
		mk, err = getIdFromBase(string(kvpair.Key))
		if err != nil || mk == 0 {
			log.Println("Parse key", string(kvpair.Key), " happened error:", err)
			continue
		}
		// log.Printf("Add [%d]=%s\n", mk, filepath.Join(s.nfs, string(kvpair.Value)))
		s.M[mk] = filepath.Join(s.nfs, string(kvpair.Value))
	}

	// watch the event from this revision
	go func() {
		watchStartRevision = getResp.Header.Revision + 1
		watchChan = s.watcher.Watch(context.TODO(), watchKey, clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())

		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case clientv3.EventTypePut:
					log.Printf("PUT Event and [%s]=%s\n", string(watchEvent.Kv.Key), string(watchEvent.Kv.Value))
					s.Lock()
					mk, err = getIdFromBase(string(watchEvent.Kv.Key))
					if err != nil || mk == 0 {
						log.Println("Parse key", string(kvpair.Key), " happened error:", err)
					} else {
						log.Printf("success PUT [%d]=%s\n", mk, filepath.Join(s.nfs, string(watchEvent.Kv.Value)))
						s.M[mk] = filepath.Join(s.nfs, string(watchEvent.Kv.Value))
					}
					s.Unlock()
				case clientv3.EventTypeDelete:
					log.Printf("Delete Event and [%s]=%s\n", string(watchEvent.Kv.Key), string(watchEvent.Kv.Value))
				}
			}
		}
	}()

	log.Println("over init")
	return
}

func InitSectorMgr(cli *cli.Context, actor string) (err error) {
	var (
		client    *clientv3.Client
		kv        clientv3.KV
		lease     clientv3.Lease
		watcher   clientv3.Watcher
		endpoints []string
		nfs       string
	)
	if cli.Bool("seal") {
		log.Println("don't cluster watch value.")
		err = nil
		return
	}

	endpoints = cli.StringSlice("cluster")
	if len(endpoints) == 0 {
		err = errors.New("cluster is empty.")
		return
	}

	nfs = cli.String("nfs")

	if client, err = NewClient(endpoints); err != nil {
		return
	}

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	G_setctorInfo = &SectorMgrInfo{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
		nfs:     nfs,
		actor:   actor,
		M:       make(map[uint64]string),
	}

	err = G_setctorInfo.loopWatch()
	return

}

func getIdFromBase(key string) (id uint64, err error) {
	id, err = strconv.ParseUint(filepath.Base(key), 10, 64)
	return
}

func NewClient(endpoints []string) (*clientv3.Client, error) {
	config := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Duration(5000) * time.Millisecond,
	}

	errChan := make(chan error, 1)
	cliChan := make(chan *clientv3.Client, 1)

	go func() {
		cli, err := clientv3.New(config)
		if err != nil {
			errChan <- err
			return
		}
		statusRes, err := cli.Status(context.Background(), config.Endpoints[0])
		if err != nil {
			errChan <- err
			return
		} else if statusRes == nil {
			errChan <- errors.New("The status response from etcd was nil")
			return
		}
		cliChan <- cli
	}()

	select {
	case err := <-errChan:
		return nil, err
	case cli := <-cliChan:
		return cli, nil
	case <-time.After(3 * time.Second):
		return nil, errors.New("A timeout occured while trying to connect to the etcd server")
	}
}
