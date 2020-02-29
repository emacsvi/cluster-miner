package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/emirpasic/gods/sets/hashset"
	"go.etcd.io/etcd/clientv3"
	"gopkg.in/urfave/cli.v2"
	"log"
	"sync"
)

const MAX_QUEUE_LENGHT = 100000

type SetInfo struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
	actor   string

	sync.RWMutex
	set  *hashset.Set
	c    chan uint64
	quit chan struct{}
}

var HeightSet *SetInfo

func InitHeightSet(cli *cli.Context, actor string) (err error) {
	var (
		client    *clientv3.Client
		kv        clientv3.KV
		lease     clientv3.Lease
		watcher   clientv3.Watcher
		endpoints []string
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

	if client, err = NewClient(endpoints); err != nil {
		return
	}

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	HeightSet = &SetInfo{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
		actor:   actor,
		set:     hashset.New(),
		c:       make(chan uint64, MAX_QUEUE_LENGHT),
		quit:    make(chan struct{}),
	}

	err = HeightSet.Load()
	log.Println("heightSet.size=", HeightSet.set.Size())

	go HeightSet.run()
	return

}

func (s *SetInfo) Stop() {
	close(s.quit)
}

func (s *SetInfo) run() {
	for {
		select {
		case height := <-s.c:
			if err := s.put(height); err != nil {
				log.Println("set put height:", height, "err:", err)
			}
		case <-s.quit:
			return
		}
	}
}

func (s *SetInfo) Add(h uint64) (err error) {
	if h <= 0 {
		return errors.New("height value must bigger than 0.")
	}
	s.Lock()
	defer s.Unlock()
	s.set.Add(h)
	s.c <- h
	return
}

func (s *SetInfo) Contains(h uint64) bool {
	s.RLock()
	defer s.RUnlock()
	return s.set.Contains(h)
}

func (s *SetInfo) put(height uint64) (err error) {
	var (
		key     string
		putResp *clientv3.PutResponse
	)
	key = BLOCK_HEIGHT_PREFIX + fmt.Sprintf("%s/%d", s.actor, height)
	if putResp, err = s.kv.Put(context.TODO(), key, "1", clientv3.WithPrevKV()); err != nil {
		log.Printf("error save to etcd [%s]=%s\n", key, "1")
		return
	}

	// log.Printf("success save to etcd [%s]=%s\n", key, "1")

	if putResp.PrevKv != nil {
		log.Println("error while put height :", height, " and k-v: ", string(putResp.PrevKv.Key), string(putResp.PrevKv.Value))
	}

	return
}

func (s *SetInfo) Load() (err error) {
	var (
		getResp  *clientv3.GetResponse
		kvpair   *mvccpb.KeyValue
		mk       uint64
		watchKey string
	)

	// /xjgw/height/t02112/72156 => 1
	watchKey = fmt.Sprintf("%s%s/", BLOCK_HEIGHT_PREFIX, s.actor)
	log.Println("watchKey=", watchKey)

	if getResp, err = s.kv.Get(context.TODO(), watchKey, clientv3.WithPrefix()); err != nil {
		return
	}

	for _, kvpair = range getResp.Kvs {
		// log.Printf("HeightSet Add [%s]=%s\n", string(kvpair.Key), string(kvpair.Value))
		mk, err = getIdFromBase(string(kvpair.Key))
		if err != nil || mk == 0 {
			log.Println("HeightSet Parse key", string(kvpair.Key), " happened error:", err)
			continue
		}
		// log.Printf("HeightSet Add (%d)", mk)
		s.set.Add(mk)
	}

	log.Println("load over")
	return
}
