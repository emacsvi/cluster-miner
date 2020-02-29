package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/clientv3"
	"gopkg.in/urfave/cli.v2"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	TEMPLATE_SIZE  = 34359738368
	MD5_PIECES_NUM = 9000
)

type IsMiner struct {
	IsSeal            bool
	NFSSealDir        string
	Srv               string
	ActorId           string
	RepoStorage       string
	OriginRepoStorage string
	Hostname          string
	cli               *SingleConnRpcClient

	// 此处增加是为了增加一个公用template数据，每次用这个共同的数据进行处理
	// 流程：
	// 1. kv里面存的是/xjgw/kvstore/commP/t02112/md5value=>0x00000512
	// 2. md5value = bigFileMd5(template)
	//   2.1 bigFileMd5()这个函数是自定义的，因为一个32G的文件md5值太慢了，我改进了一下算法，只取前面的几千或者几万个buffer数据出来进行运算即可。
	// 3. 程序每次启动之后去查看是否有template文件，如果没有，则走正常流程生成template,并且将结果更新到kv里面
	// 如果有则计算md5值，去kv里面查找commP结果，如果找不到, 则删除template文件，继续走正常流程
	// 如果有而且计算出来的值能找到commP的结果。即正常创建软连接进行处理
	client               *clientv3.Client
	kv                   clientv3.KV
	lease                clientv3.Lease
	watcher              clientv3.Watcher
	isNeedCreateTemplate bool
	sync.RWMutex
	commP []byte
}

func (m *IsMiner) String() string {
	return fmt.Sprintf("<IsSeal:%v, Srv:%s, ActorId:%s, RepoStorage:%s, Hostname:%s, NFSSealDir:%s>",
		m.IsSeal, m.Srv, m.ActorId, m.RepoStorage, m.Hostname, m.NFSSealDir)
}

var (
	MinerFlag *IsMiner
)

func InitMinerFlag(cli *cli.Context, actor string) (err error) {
	var (
		isSeal     bool
		nfs        string
		srv        string
		repo       string
		originRepo string
		hostname   string
		client     *clientv3.Client
		kv         clientv3.KV
		lease      clientv3.Lease
		watcher    clientv3.Watcher
		endpoints  []string
	)

	isSeal = cli.Bool("seal")
	if !isSeal {
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

	nfs = cli.String("nfs")
	srv = cli.String("srv")
	originRepo = cli.String("storagerepo")

	repo = getBase(originRepo)

	hostname, err = Hostname()
	if err != nil {
		log.Fatalln("hostname must seted.", err)
		return
	}
	MinerFlag = &IsMiner{
		IsSeal:            isSeal,
		NFSSealDir:        nfs,
		Srv:               srv,
		RepoStorage:       repo,
		OriginRepoStorage: originRepo,
		ActorId:           actor,
		Hostname:          hostname,

		// added for commp
		client:               client,
		kv:                   kv,
		lease:                lease,
		watcher:              watcher,
		isNeedCreateTemplate: true,
	}

	if isSeal {
		MinerFlag.cli = &SingleConnRpcClient{
			RpcServer: srv,
			Timeout:   time.Duration(10) * time.Millisecond,
		}
	}

	err = MinerFlag.load()
	log.Println("over and m.isNeedCreateFile=", MinerFlag.isNeedCreateTemplate)

	return
}

func (m *IsMiner) load() error {
	var (
		getResp  *clientv3.GetResponse
		kvpair   *mvccpb.KeyValue
		watchKey string
		tmp      string
		err      error
		fi       os.FileInfo
		md5      string
	)
	// init commP info
	// 1. 判断template文件是否存在
	tmp = filepath.Join(m.OriginRepoStorage, "/staging/template")
	fi, err = os.Stat(tmp)
	if err != nil || fi.Size() != TEMPLATE_SIZE {
		// 文件不存在，则需要重新创建template
		log.Printf("cannot find %s or err:%v or size not match. so we will create template\n", tmp, err)
		_ = os.Remove(tmp)
		m.isNeedCreateTemplate = true
		return nil
	}

	// 文件存在且大小是32G
	// 计算md5值，并且到kv中去查找commP值
	md5, err = MD5sum(tmp, MD5_PIECES_NUM)
	if err != nil || md5 == "" {
		return errors.New("Md5sum file " + tmp + "happen error.")
	}

	watchKey = fmt.Sprintf("%s%s/%s", COMMP_PREFIX, m.ActorId, md5)
	log.Println("load watchKey=", watchKey)

	if getResp, err = m.kv.Get(context.TODO(), watchKey); err != nil {
		return err
	}

	for _, kvpair = range getResp.Kvs {
		log.Printf("commP add [%s]=[0x%x]\n", string(kvpair.Key), kvpair.Value)
		log.Printf("success commP add [%s]=[0x%x]\n", string(kvpair.Key), kvpair.Value)
		m.commP = kvpair.Value
		break
	}

	// 有文件，但是找不到对应的commP 则删除文件，重新创建
	if len(m.commP) <= 0 {
		m.isNeedCreateTemplate = true

		log.Println("find empty.")
		if err = os.Remove(tmp); err != nil {
			return err
		}
		return nil
	}

	// 有文件，并且成功找到了对应的commP
	m.isNeedCreateTemplate = false
	log.Printf("success get commP:0x%x\n", m.commP)
	return nil
}

func (m *IsMiner) AcquireSectorId() (uint64, error) {
	m.RLock()
	defer m.RUnlock()
	var err error
	req := AgentReportRequest{
		Hostname:     m.Hostname,
		Repo:         m.RepoStorage,
		ActorId:      m.ActorId,
		IP:           "127.0.0.1",
		AgentVersion: "0.0.2",
	}
	var resp AgentSectorIdResponse
	err = m.cli.Call("Agent.GetSectorId", req, &resp)
	if err != nil || resp.SectorId == 0 {
		log.Println("call agent.getsectorid error: ", err)
		err = errors.New("Agent.GetSectorId failed.")
		return 0, err
	}
	log.Println("success get id: ", resp.SectorId)
	err = m.createSymLink(resp.SectorId)
	if err != nil {
		log.Println("createSymLink error:", err)
	}
	return resp.SectorId, nil
}

func (m *IsMiner) createSymLink(id uint64) error {
	if m.isNeedCreateTemplate {
		log.Println("need to create template so nothing to do.")
		return nil
	}
	symlink := filepath.Join(m.OriginRepoStorage, fmt.Sprintf("/staging/s-%s-%d", m.ActorId, id))
	log.Println("createSymLink:", symlink)
	return os.Symlink("template", symlink)
}

// 在创建好所有消息，并且创建完成文件之后调用save操作
// 将对应的sectorId 复制为template文件，并且将md5=>commP上传到集群
func (m *IsMiner) Save(id uint64, commP string) error {
	m.Lock()
	defer m.Unlock()

	var (
		key     string
		putResp *clientv3.PutResponse
		tmp     string
		err     error
		fi      os.FileInfo
		md5     string
	)

	if err = m.CreateTemplate(id); err != nil {
		log.Println("CreateTemplate error:", err)
		return err
	}

	// 1. 判断template文件是否存在
	tmp = filepath.Join(m.OriginRepoStorage, "/staging/template")
	fi, err = os.Stat(tmp)
	if err != nil || fi.Size() != TEMPLATE_SIZE {
		// 文件不存在，则需要重新创建template
		log.Printf("err:%v or size not match. so we will create template\n", err)
		_ = os.Remove(tmp)
		m.isNeedCreateTemplate = true
		return nil
	}

	// 文件存在且大小是32G
	// 计算md5值，并且到kv中去查找commP值
	md5, err = MD5sum(tmp, MD5_PIECES_NUM)
	if err != nil || md5 == "" {
		return errors.New("Md5sum file " + tmp + "happen error.")
	}

	key = fmt.Sprintf("%s%s/%s", COMMP_PREFIX, m.ActorId, md5)
	log.Println("save key=", key)

	if putResp, err = m.kv.Put(context.TODO(), key, commP, clientv3.WithPrevKV()); err != nil {
		log.Printf("error save to etcd [%s]=(0x%x)\n", key, []byte(commP))
		return err
	}

	m.isNeedCreateTemplate = false
	m.commP = []byte(commP)

	log.Printf("success save [%s]=(0x%x)\n", key, []byte(commP))

	if putResp.PrevKv != nil {
		log.Println("error PreKv while put k new value:", commP, " and old k-v: ", string(putResp.PrevKv.Key), string(putResp.PrevKv.Value))
	}

	return nil
}

func (m *IsMiner) GetCommP() []byte {
	m.RLock()
	defer m.RUnlock()

	if m.isNeedCreateTemplate {
		return nil
	}

	// 做一次深拷贝，以免外界干扰正常内容
	if len(m.commP) <= 0 {
		return nil
	}
	r := make([]byte, len(m.commP))
	if n := copy(r, m.commP); n <= 0 {
		return nil
	}
	return r[:]
}

func (m *IsMiner) CreateTemplate(id uint64) error {
	if m.OriginRepoStorage == "" {
		return errors.New("RepoStorage is empty.")
	}

	source := filepath.Join(m.OriginRepoStorage, fmt.Sprintf("/staging/s-%s-%d", m.ActorId, id))
	destination := filepath.Join(m.OriginRepoStorage, "/staging/template")
	log.Println("source:", source)
	log.Println("destination:", destination)

	var (
		size int64
		err  error
	)

	if size, err = getFileSize(source); err != nil {
		log.Printf("getFileSize(%s) error:%v", source, err)
		return err
	}

	if size != TEMPLATE_SIZE {
		log.Printf("getFileSize(%s) size !=%d", source, TEMPLATE_SIZE)
		return errors.New("file size error.")
	}

	_ = os.Remove(destination)

	err = copyfile(source, destination, 1024*1024*1024*8)
	if err != nil {
		log.Printf("copy %s to %s error: %v\n", source, destination, err)
	} else {
		log.Printf("success copy %s to %s\n", source, destination)
	}

	return err
}

func getFileSize(f string) (int64, error) {
	fi, err := os.Stat(f)
	if err != nil {
		return 0, err
	}

	log.Println("size:", fi.Size())
	return fi.Size(), err
}

func copyfile(src, dst string, buffSize int64) error {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		log.Println("stat error:", err)
		return err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file.", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	_, err = os.Stat(dst)
	if err == nil {
		return fmt.Errorf("File %s already exists.", dst)
	}

	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destination.Close()

	if err != nil {
		return err
	}

	buf := make([]byte, buffSize)
	for {
		n, err := source.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		if _, err := destination.Write(buf[:n]); err != nil {
			return err
		}
	}
	return err
}

func getBase(path string) (base string) {
	base = filepath.Base(path)
	base = strings.Trim(base, ".")
	return
}

func Hostname() (hostname string, err error) {
	hostname, err = os.Hostname()
	return
}
