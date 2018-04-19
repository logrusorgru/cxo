package db

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/skycoin/net/skycoin-messenger/factory"
	"github.com/skycoin/skycoin/src/cipher"
)

type Node struct {
	ID             int64
	PK             string
	ServiceAddress string
	Location       string
	Version        []string
	Priority       int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Scan *sql.Rows or *sql.Row
func (n *Node) Scan(row sql.Scanner) (err error) {
	//
	return
}

type Service struct {
	ID                int64
	PK                string
	Address           string
	HideFromDiscovery bool
	AllowNodes        []string
	Version           string
	NodeId            int64
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type Attributes struct {
	Name      string
	ServiceID int64
}

/*

func (d *DB) servicesIDsByNodeID(
	tx *sql.Tx, //   :
	nodeID int64, // :
) (
	sis []int64, //  : ids of services
	err error, //    :
) {

	var rows *sql.Rows
	rows, err = tx.Query(`SELECT id FROM service WHERE node_id = ?;`, nodeID)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return
		}
		sis = append(sis, id)
	}

	err = rows.Err()
	return
}

*/

func (d *DB) UnRegisterService(pk cipher.PubKey) (err error) {

	var tx *sql.Tx
	if tx, err = d.db.Begin(); err != nil {
		return
	}

	_, err = tx.Exec(`DELETE FROM node WHERE pk = ?;`, pk.Hex())
	if err != nil {
		tx.Rollback()
		return
	}

	return tx.Commit()
}

func serializeStrings(ss []string) (s string, err error) {
	var sb []byte
	if sb, err = json.Marshal(ss); err != nil {
		return
	}
	s = string(sb)
	return
}

func deserializeStrings(s string) (ss []string, err error) {
	err = json.Unmarshal([]byte(s), &ss)
	return
}

// returns (0, nil) if not exist
func (d *DB) nodeIDByPK(tx *sql.Tx, pk cipher.PubKey) (id int64, err error) {
	err = tx.QueryRow(`SELECT id FROM node WHERE pk = ?;`, pk.Hex()).
		Scan(&id)
	if err == sql.ErrNoRows {
		err = nil // id = 0
	}
	return
}

func (d *DB) updateNode(
	tx *sql.Tx, //            :
	nodeID int64, //          :
	serviceAddress string, // :
	location string, //       :
	version string, //        :
) (
	err error, //             :
) {

	const updateNode = `UPDATE node
        SET
          service_address = ?,
          location = ?,
          version = ?,
          updated_at = ?
        WHERE id = ?;`

	_, err = tx.Exec(updateNode,
		serviceAddress,
		location,
		version,
		time.Now(),
		nodeID)

	return
}

func (d *DB) insertNode(
	tx *sql.Tx,
	pk cipher.PubKey,
	serviceAddresses string,
	location string,
	version string,
) (
	nodeID int64,
	err error,
) {

	const insertNode = `INSERT INTO node (
      pk,
      service_address,
      location,
      version,

      created_at,
      updated_at
    ) VALUES (?, ?, ?, ?, ?, ?);`

	var (
		now    = time.Now()
		result sql.Result
	)

	result, err = tx.Exec(insertNode,
		pk.Hex(),
		serviceAddresses,
		location,
		version,
		now,
		now)
	if err != nil {
		return
	}

	nodeID, err = result.LastInsertId()
	return
}

func (d *DB) updateService(
	tx *sql.Tx, //             :
	serviceID int64, //        :
	pk cipher.PubKey, //       :
	address string, //         :
	hideFromDiscovery bool, // :
	allowNodes string, //      :
	version string, //         :
	nodeID int64, //           :
) (
	err error, //              :
) {

	const updateService = `UPDATE service
        SET
          pk = ?,
          address = ?,
          hide_from_discovery = ?,
          allow_nodes = ?,
          version = ?,

          node_id = ?,

          updated_at = ?
        WHERE id = ?;`

	_, err = tx.Exec(updateService,
		pk.Hex(),
		address,
		hideFromDiscovery,
		allowNodes,
		version,
		nodeID, // node_id? does it really needed?
		time.Now(),
		serviceID)
	return
}

func (d *DB) insertService(
	tx *sql.Tx, //             :
	pk cipher.PubKey, //       :
	address string, //         :
	hideFromDiscovery bool, // :
	allowNodes string, //      :
	version string, //         :
	nodeID int64, //           :
) (
	serviceID int64, //        :
	err error, //              :
) {

	const insertService = `INSERT INTO service (
      pk,
      address,
      hide_from_discovery,
      allow_nodes,
      version,

      node_id,

      created_at,
      updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);`

	var (
		now    = time.Now()
		result sql.Result
	)

	result, err = tx.Exec(insertService,
		pk.Hex(),
		address,
		hideFromDiscovery,
		allowNodes,
		version,
		nodeID,
		now,
		now)
	if err != nil {
		return
	}

	serviceID, err = result.LastInsertId()
	return
}

// returns (0, nil) if not exist
func (d *DB) serviceIDByPK(tx *sql.Tx, pk cipher.PubKey) (id int64, err error) {
	err = tx.QueryRow(`SELECT id FROM service WHERE pk = ?;`, pk.Hex()).
		Scan(&id)
	if err == sql.ErrNoRows {
		err = nil // id = 0
	}
	return
}

func (d *DB) RegisterService(
	pk cipher.PubKey,
	ns *factory.NodeServices,
) (
	err error,
) {

	var tx *sql.Tx
	if tx, err = d.db.Begin(); err != nil {
		return
	}

	var nodeID int64
	if nodeID, err = d.nodeIDByPK(tx, pk); err != nil {
		tx.Rollback()
		return
	}

	var version string
	if version, err = serializeStrings(ns.Version); err != nil {
		tx.Rollback()
		return
	}

	// if exists
	if nodeID != 0 {
		err = d.updateNode(tx, nodeID, ns.ServiceAddress, ns.Location, version)
	} else {
		nodeID, err = d.insertNode(tx,
			pk,
			ns.ServiceAddress,
			ns.Location,
			version)
	}

	if err != nil {
		tx.Rollback()
		return
	}

	// TODO (kostyarin): -----

	for _, v := range ns.Services {

		var serviceID int64
		if serviceID, err = d.serviceIDByPK(tx, v.Key.Hex()); err != nil {
			tx.Rollback()
			return
		}

		var allowNodes string
		if allowNodes, err = serializeStrings(v.AllowNodes); err != nil {
			tx.Rollback()
			return
		}

		// if exists
		if serviceID != 0 {

			err = d.updateService(tx,
				serviceID,
				v.Key,
				v.Address,
				v.HideFromDiscovery,
				allowNodes,
				v.Version,
				nodeID)
			if err != nil {
				tx.Rollback()
				return
			}

			const deleteAttribute = `DELETE FROM attribute
            WHERE service_id = ?;`

			if _, err = tx.Exec(deleteAttribute, serviceID); err != nil {
				tx.Rollback()
				return
			}

		} else {

			serviceID, err = d.insertService(tx,
				v.Key,
				v.Address,
				v.HideFromDiscovery,
				allowNodes,
				v.Version,
				nodeID)

			if err != nil {
				tx.Rollback()
				return
			}

		}

		for _, attr := range v.Attributes {

			const insertAttribute = `INSERT INTO attribute
              (name, service_id)
            VALUES
              (?, ?);`

			if _, err = tx.Exec(insertAttribute, attr, serviceID); err != nil {
				tx.Rollback()
				return
			}
		}

	}

	return tx.Commit()
}

type NodeDetail struct {
	Node
	Service
	Attributes
}

func FindResultByAttrs(attr ...string) (result *factory.AttrNodesInfo) {

	sas := make([]NodeDetail, 0)
	err := engine.Join("INNER", "service", "service.node_id = node.id").
		Join("INNER", "attributes", "attributes.service_id = service.id").
		In("attributes.name", attr).
		Desc("node.priority").
		Table("node").
		Find(&sas)
	if err != nil {
		return
	}

	atis := make(map[string]*factory.AttrNodeInfo)
	for _, v := range sas {
		nodeKey, err := cipher.PubKeyFromHex(v.Node.Key)
		if err != nil {
			continue
		}
		appKey, err := cipher.PubKeyFromHex(v.Service.Key)
		if err != nil {
			continue
		}
		ati, ok := atis[v.Service.Key]
		if ok {
			ati.AppInfos = append(ati.AppInfos, &factory.AttrAppInfo{})
			atis[v.Service.Key] = ati
		} else {

			appinfos := make([]*factory.AttrAppInfo, 0)
			appinfos = append(appinfos, &factory.AttrAppInfo{
				Key:     appKey,
				Version: v.Service.Version,
			})
			apps := make([]cipher.PubKey, 0)
			appsKey, err := cipher.PubKeyFromHex(v.Service.Key)
			if err != nil {
				continue
			}
			apps = append(apps, appsKey)
			info := &factory.AttrNodeInfo{
				Node:     nodeKey,
				Apps:     apps,
				Location: v.Node.Location,
				Version:  v.Node.Version,
				AppInfos: appinfos,
			}
			atis[v.Service.Key] = info
		}
	}
	result = &factory.AttrNodesInfo{
		Nodes: make([]*factory.AttrNodeInfo, 0),
	}
	for _, v := range atis {
		result.Nodes = append(result.Nodes, v)
	}
	return
}

func FindResultByAttrsAndPaging(
	pages, limit int,
	attr ...string,
) (
	result *factory.AttrNodesInfo,
) {

	sas := make([]NodeDetail, 0)

	err := engine.Join("INNER", "service", "service.node_id = node.id").
		Join("INNER", "attributes", "attributes.service_id = service.id").
		In("attributes.name", attr).
		Limit(limit, (pages-1)*limit).
		Desc("node.priority").
		Table("node").
		Find(&sas)

	if err != nil {
		return
	}

	atis := make(map[string]*factory.AttrNodeInfo)
	for _, v := range sas {
		nodeKey, err := cipher.PubKeyFromHex(v.Node.Key)
		if err != nil {
			continue
		}
		appKey, err := cipher.PubKeyFromHex(v.Service.Key)
		if err != nil {
			continue
		}
		ati, ok := atis[v.Service.Key]
		if ok {
			ati.AppInfos = append(ati.AppInfos, &factory.AttrAppInfo{})
			atis[v.Service.Key] = ati
		} else {
			appinfos := make([]*factory.AttrAppInfo, 0)
			appinfos = append(appinfos, &factory.AttrAppInfo{
				Key:     appKey,
				Version: v.Service.Version,
			})
			apps := make([]cipher.PubKey, 0)
			appsKey, err := cipher.PubKeyFromHex(v.Service.Key)
			if err != nil {
				continue
			}
			apps = append(apps, appsKey)
			info := &factory.AttrNodeInfo{
				Node:     nodeKey,
				Apps:     apps,
				Location: v.Node.Location,
				Version:  v.Node.Version,
				AppInfos: appinfos,
			}
			atis[v.Service.Key] = info
		}
	}
	count, err := engine.Join("INNER", "service", "service.node_id = node.id").
		Join("INNER", "attributes", "attributes.service_id = service.id").
		In("attributes.name", attr).
		Count(new(Node))
	if err != nil {
		return
	}
	result = &factory.AttrNodesInfo{
		Nodes: make([]*factory.AttrNodeInfo, 0),
		Count: count,
	}
	for _, v := range atis {
		result.Nodes = append(result.Nodes, v)
	}
	return
}

type NodeAndService struct {
	Node
	Service
}

func FindServiceAddresses(
	keys []cipher.PubKey,
	exclude cipher.PubKey,
) (
	result []*factory.ServiceInfo,
) {

	appKeys := make([]string, len(keys))
	for _, v := range keys {
		appKeys = append(appKeys, v.Hex())
	}

	excludeNodeKey := exclude.Hex()
	ns := make([]NodeAndService, 0)
	err := engine.Join("INNER", "service", "service.node_id = node.id").
		Where("node.key != ?", excludeNodeKey).
		In("service.key", appKeys).
		Table("node").
		Find(&ns)
	if err != nil {
		return
	}

	ss := make(map[string][]*factory.NodeInfo)
	for _, v := range ns {
		nodeKey, err := cipher.PubKeyFromHex(v.Node.Key)
		if err != nil {
			continue
		}
		node := &factory.NodeInfo{
			PubKey:  nodeKey,
			Address: v.Node.ServiceAddress,
		}
		s, ok := ss[v.Service.Key]
		if ok {
			s = append(s, node)
			ss[v.Service.Key] = s
		} else {
			nodes := make([]*factory.NodeInfo, 0)
			nodes = append(nodes, node)
			ss[v.Service.Key] = nodes
		}
	}

	result = make([]*factory.ServiceInfo, 0)
	for k, v := range ss {
		serviceKey, err := cipher.PubKeyFromHex(k)
		if err != nil {
			continue
		}
		result = append(result, &factory.ServiceInfo{
			PubKey: serviceKey,
			Nodes:  v,
		})
	}

	return
}
