package cluster

import (
	"fmt"
)

type AgentReportRequest struct {
	Hostname     string
	Repo         string
	ActorId      string
	IP           string
	AgentVersion string
}

func (a *AgentReportRequest) String() string {
	return fmt.Sprintf(
		"<Hostname:%s, Repo:%s, IP:%s, AgentVersion:%s>",
		a.Hostname,
		a.Repo,
		a.IP,
		a.AgentVersion,
	)
}

type AgentSectorIdResponse struct {
	SectorId  uint64
	Timestamp int64
}

func (r *AgentSectorIdResponse) String() string {
	return fmt.Sprintf("<SectorId:%d, Timestamp:%d>", r.SectorId, r.Timestamp)
}

const (

	/*
			  每一个actor所有的id 都对应一个目录, 因为id可能在不同的目录之中， 所以在miner之中监听的时候需要下到actorsid那一层目录中去监听
			put:
				/xjgw/sectors/actorid/sectorid    hostname-repo
			watch:
				/xjgw/sectors/actorid

		[/xjgw/sectors/t01021/1576]=xjgw3960-r3960
		2020-01-06 10:55:36.948888 I | PUT Event and [/xjgw/sectors/t01073/7629]=lotus-lotusstorage
	*/
	SECTORS_KEY_PREFIX = "/xjgw/sectors/"
	/*
	   每一个actor对应一个自增长的id, 平台为了给每个work分配sectorid用的
	   类似：
	       /xjgw/actors/t01073 -> 55
	       /xjgw/actors/t02021 -> 195

	*/
	ACTORS_KEY_PREFIX = "/xjgw/actors/"

	// save block height number
	// /xjgw/height/t02112/72156 => 1
	BLOCK_HEIGHT_PREFIX = "/xjgw/height/"
)
