package cluster

import (
	"errors"
	"fmt"
	"gopkg.in/urfave/cli.v2"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type IsMiner struct {
	IsSeal      bool
	NFSSealDir  string
	Srv         string
	ActorId     string
	RepoStorage string
	Hostname    string
	cli         *SingleConnRpcClient
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
		isSeal   bool
		nfs      string
		srv      string
		repo     string
		hostname string
	)
	isSeal = cli.Bool("seal")
	if !isSeal {
		return
	}

	nfs = cli.String("nfs")
	srv = cli.String("srv")
	repo = cli.String("storagerepo")
	repo = getBase(repo)

	hostname, err = Hostname()
	if err != nil {
		log.Fatalln("hostname must seted.", err)
		return
	}
	MinerFlag = &IsMiner{
		IsSeal:      isSeal,
		NFSSealDir:  nfs,
		Srv:         srv,
		RepoStorage: repo,
		ActorId:     actor,
		Hostname:    hostname,
	}
	if isSeal {
		MinerFlag.cli = &SingleConnRpcClient{
			RpcServer: srv,
			Timeout:   time.Duration(10) * time.Millisecond,
		}
	}
	return
}

/*
func getActor(cctx *cli.Context) (actor string, err error) {
	nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
	if err != nil {
		return
	}
	defer closer()

	ctx := lcli.ReqContext(cctx)

	maddr, err := nodeApi.ActorAddress(ctx)
	if err != nil {
		return
	}
	actor = maddr.String()
	return
}

*/

func getBase(path string) (base string) {
	base = filepath.Base(path)
	base = strings.Trim(base, ".")
	return
}

func Hostname() (hostname string, err error) {
	hostname, err = os.Hostname()
	return
}

func (m *IsMiner) AcquireSectorId() (uint64, error) {
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
	return resp.SectorId, nil
}
