package server

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	r, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	val, err := r.GetCF(req.GetCf(), req.GetKey())
	resp := &kvrpcpb.RawGetResponse{}

	if err == nil && val == nil {
		resp.NotFound = true
		return resp, nil
	} else if err != nil {
		return nil, err
	}

	resp.Value = val
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var ops []storage.Modify
	ops = append(ops, storage.Modify{Data: storage.Put{
		Cf:    req.GetCf(),
		Key:   req.GetKey(),
		Value: req.GetValue(),
	}})

	err := server.storage.Write(nil, ops)
	resp := &kvrpcpb.RawPutResponse{}
	return resp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var ops []storage.Modify
	ops = append(ops, storage.Modify{Data: storage.Delete{
		Cf:  req.GetCf(),
		Key: req.GetKey(),
	}})

	err := server.storage.Write(nil, ops)
	resp := &kvrpcpb.RawDeleteResponse{}
	return resp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	r, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	resp := kvrpcpb.RawScanResponse{}

	iter := r.IterCF(req.GetCf())
	fmt.Printf(" scan cf %s start %s limt %d ", req.Cf, req.StartKey, req.Limit)
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		val, _ := iter.Item().Value()
		if len(resp.Kvs) < int(req.Limit) {
			resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
				Key:   iter.Item().Key(),
				Value: val,
			})
		}
	}
	iter.Close()
	r.Close()
	return &resp, nil
}
