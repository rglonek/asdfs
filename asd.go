package main

import "github.com/aerospike/aerospike-client-go/v8"

var MRTEnabled = false

type MRT struct {
	txn    *aerospike.Txn
	read   *aerospike.BasePolicy
	write  *aerospike.WritePolicy
	client *aerospike.Client
}

func GetReadPolicyNoMRT(client *aerospike.Client, t *cfgTimeout) *aerospike.BasePolicy {
	read := aerospike.NewPolicy()
	read.TotalTimeout = t.Total
	read.SocketTimeout = t.Socket
	return read
}

func GetWritePolicyNoMRT(client *aerospike.Client, t *cfgTimeout) *aerospike.WritePolicy {
	write := aerospike.NewWritePolicy(0, 0)
	write.DurableDelete = true
	write.SendKey = true
	write.TotalTimeout = t.Total
	write.SocketTimeout = t.Socket
	return write
}

func GetReadPolicy(client *aerospike.Client, t *cfgTimeout) *MRT {
	txn := aerospike.NewTxn()
	txn.SetTimeout(t.MRT)
	if !MRTEnabled {
		txn = nil
	}
	m := &MRT{
		txn:    txn,
		read:   aerospike.NewPolicy(),
		client: client,
	}
	m.read.TotalTimeout = t.Total
	m.read.SocketTimeout = t.Socket
	m.read.Txn = txn
	return m
}

func GetPolicies(client *aerospike.Client, t *cfgTimeout) *MRT {
	txn := aerospike.NewTxn()
	txn.SetTimeout(t.MRT)
	if !MRTEnabled {
		txn = nil
	}
	m := &MRT{
		txn:    txn,
		read:   aerospike.NewPolicy(),
		write:  aerospike.NewWritePolicy(0, 0),
		client: client,
	}
	m.read.Txn = txn
	m.read.TotalTimeout = t.Total
	m.read.SocketTimeout = t.Socket
	m.write.TotalTimeout = t.Total
	m.write.SocketTimeout = t.Socket
	m.write.Txn = txn
	m.write.DurableDelete = true
	m.write.SendKey = true
	return m
}

func GetWritePolicy(client *aerospike.Client, t *cfgTimeout) *MRT {
	txn := aerospike.NewTxn()
	txn.SetTimeout(t.MRT)
	if !MRTEnabled {
		txn = nil
	}
	m := &MRT{
		txn:    txn,
		write:  aerospike.NewWritePolicy(0, 0),
		client: client,
	}
	m.write.Txn = txn
	m.write.DurableDelete = true
	m.write.SendKey = true
	m.write.TotalTimeout = t.Total
	m.write.SocketTimeout = t.Socket
	return m
}

func (m *MRT) Commit() error {
	if !MRTEnabled {
		return nil
	}
	_, err := m.client.Commit(m.txn)
	return err
}

func (m *MRT) Abort() error {
	if !MRTEnabled {
		return nil
	}
	_, err := m.client.Abort(m.txn)
	return err
}

func (m *MRT) Read() *aerospike.BasePolicy {
	return m.read
}

func (m *MRT) Write() *aerospike.WritePolicy {
	return m.write
}
