package db

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

// returns "?,?,?"" depending of the count that shuldn't be 0
func sqlInParams(count int) string {

	if count == 0 {
		return ""
	}

	var b = make([]byte, 0, count*2-1)

	for i := 0; i < count; i++ {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '?')
	}

	return string(b)
}

func stringsToInterfaces(ss []string) (is []interface{}) {

	if len(ss) == 0 {
		return
	}

	is = make([]interface{}, 0, len(ss))

	for _, s := range ss {
		is = append(is, interface{}(s))
	}

	return
}

func pubKeyFromHex(pks string) (pk cipher.PubKey, err error) {
	var b []byte
	if b, err = hex.DecodeString(pks); err != nil {
		return
	}
	if len(b) != len(cipher.PubKey{}) {
		err = errors.New("invalid PubKey length")
	}
	pk = cipher.NewPubKey(b)
	return
}

// TODO (kostyarin): error ignored? log?
//
// if offset is 0 and limit is 0, then
// no LIMIT used
func (d *DB) findResultByAttrs(
	offset int, //                    :
	limit int, //                     :
	attrs ...string, //               :
) (
	result *factory.AttrNodesInfo, // :
	err error, //                     :
) {

	const selFormat = `SELECT
      node.pk,
      node.location,
      node.version,
      service.pk,
      service.version,
    FROM node
    INNER JOIN service ON service.node_id = node.id
    INNER JOIN attribute ON attribute.service_id = service.id
    WHERE attribute.name IN (%s)
    %s
    ORDER BY node.priority
    DESC;`

	if len(attrs) == 0 {
		return // nothing to search for
	}

	var (
		sel  string
		rows *sql.Rows

		inParams = sqlInParams(len(attrs))
		args     = stringsToInterfaces(attrs...)
	)

	if limit == 0 && offset == 0 {
		sel = fmt.Sprintf(selFormat, inParams, "")
	} else {
		sel = fmt.Sprintf(selFormat, inParams, "LIMIT ?, ?")
		args = append(args, offset, limit)
	}

	if rows, err = tx.Query(selFormat, args...); err != nil {
		return
	}
	defer rows.Close()

	var atis = make(map[cipher.PubKey]*factory.AttrNodeInfo)

	for rows.Next() == true {

		var (
			nodePK, nodeLocation, nodeVersion string
			servicePK, serviceVersion         string
		)

		err = rows.Scan(&nodePK, &nodeLocation, &nodeVersion, &servicePK,
			&serviceVersion)
		if err != nil {
			return
		}

		var nodeKey, appKey cipher.PubKey

		if nodeKey, err = pubKeyFromHex(nodePK); err != nil {
			continue // ?
		}
		if appKey, err = pubKeyFromHex(servicePK); err != nil {
			continue // ?
		}

		var nodeVersions []string
		if nodeVersions, err = deserializeStrings(nodeVersion); err != nil {
			return
		}

		var ati, ok = atis[appKey]

		//
		// TODO (kostyarin): what the hell is below?
		//

		if ok == true {
			ati.AppInfos = append(ati.AppInfos, &factory.AttrAppInfo{})
		} else {
			atis[appKey] = &factory.AttrNodeInfo{
				Node:     nodeKey,
				Apps:     []cipher.PubKey{appKey},
				Location: nodeLocation,
				Version:  nodeVersions,
				AppInfos: []*factory.AttrAppInfo{
					&factory.AttrAppInfo{
						Key:     appKey,
						Version: serviceVersion,
					},
				},
			}
		}

	}

	if err = rows.Err(); err != nil {
		return
	}

	// count

	if offset != 0 || limit != 0 {
		const selCountFormat = `SELECT
        COUNT (*)
        FROM node
        INNER JOIN service ON service.node_id = node.id
        INNER JOIN attribute ON attribute.service_id = service.id
        WHERE attribute.name IN (%s);`
		sel = fmt.Sprintf(selCountFormat, inParams)
		args = args[:len(args)-2] // remove offset and limit from the args
		if err = d.db.QueryRow(sel, args...).Scan(&result.Count); err != nil {
			return
		}
	}

	result = &factory.AttrNodesInfo{
		Nodes: make([]*factory.AttrNodeInfo, 0, len(atis)),
	}

	for _, ati := range atis {
		result.Nodes = append(result.Nodes, ati)
	}

	return
}

// TODO (kostyarin): handle error
func (d *DB) FindResultByAttrs(
	attrs ...string,
) (
	result *factory.AttrNodesInfo,
) {

	var err error
	if result, err = d.findResultByAttrs(0, 0, attrs...); err != nil {
		// TODO (kostyarin): log about the error
		return
	}

	return
}

func FindResultByAttrsAndPaging(
	pages, limit int,
	attr ...string,
) (
	result *factory.AttrNodesInfo,
) {

	var err error
	result, err = d.findResultByAttrs((pages-1)*limit, limit, attrs...)
	if err != nil {
		// TODO (kostyarin): log about the error
		return
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
