package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	sReader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer sReader.Close()

	val, err := sReader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}

	rawGetRsp := kvrpcpb.RawGetResponse{
		Value: val,
	}
	if val == nil {
		rawGetRsp.NotFound = true
	}
	return &rawGetRsp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}

	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	}

	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	res := &kvrpcpb.RawScanResponse{}
	if req.Limit == 0 {
		return res, nil
	}

	sReader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer sReader.Close()

	iter := sReader.IterCF(req.Cf)
	defer iter.Close()

	for iter.Seek(req.StartKey); iter.Valid() && len(res.Kvs) < int(req.Limit); iter.Next() {
		item := iter.Item()
		k := item.Key()
		v, err := item.Value()
		if err != nil {
			return nil, err
		}
		res.Kvs = append(res.Kvs, &kvrpcpb.KvPair{Key: k, Value: v})
	}

	return res, nil
}
