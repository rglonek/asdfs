package main

import "github.com/aerospike/aerospike-client-go/v8"

var MRTEnabled = false

type MRT struct {
	txn    *aerospike.Txn
	read   *aerospike.BasePolicy
	write  *aerospike.WritePolicy
	client *aerospike.Client
}

func GetReadPolicy(client *aerospike.Client) *MRT {
	txn := aerospike.NewTxn()
	if !MRTEnabled {
		txn = nil
	}
	m := &MRT{
		txn:    txn,
		read:   aerospike.NewPolicy(),
		client: client,
	}
	m.read.Txn = txn
	return m
}

func GetPolicies(client *aerospike.Client) *MRT {
	txn := aerospike.NewTxn()
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
	m.write.Txn = txn
	m.write.DurableDelete = true
	m.write.SendKey = true
	return m
}

func GetWritePolicy(client *aerospike.Client) *MRT {
	txn := aerospike.NewTxn()
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
