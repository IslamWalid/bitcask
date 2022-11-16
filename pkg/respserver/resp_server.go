// Package respserver provides implements resp server using a bitcask datastore.
package respserver

import (
	"errors"

	"github.com/IslamWalid/bitcask"
	"github.com/tidwall/resp"
)

// errInvalidArgsNum is return whenever something wrong with arguments number.
var errInvalidArgsNum = errors.New("invalid number of arguments passed")

// RespServer represents the server object.
// RespServer contains the metadata needed to manage the server.
type RespServer struct {
	port         string
	server       *resp.Server
	bitcask      *bitcask.Bitcask
	dataStoreDir string
}

// New creates new resp server object listening in the given port
// and using a datastore in the given directory path.
func New(dataStoreDir, port string) (*RespServer, error) {
	bitcask, err := bitcask.Open(dataStoreDir, bitcask.ReadWrite)
	if err != nil {
		return nil, err
	}

	return &RespServer{
		port:         port,
		server:       resp.NewServer(),
		bitcask:      bitcask,
		dataStoreDir: dataStoreDir,
	}, nil
}

// ListenAndServe registers the needed handlers then starts the server.
func (r *RespServer) ListenAndServe() error {
	r.registerHandlers()
	err := r.server.ListenAndServe(r.port)
	if err != nil {
		return err
	}

	return nil
}

// Close closes the used bitcask datastore.
func (r *RespServer) Close() {
	r.bitcask.Close()
}

// registerHandlers register the callback methods to the server.
func (r *RespServer) registerHandlers() {
	r.server.HandleFunc("set", r.set)
	r.server.HandleFunc("get", r.get)
	r.server.HandleFunc("del", r.del)
}

// set implements the callback method that handles set requests.
func (r *RespServer) set(conn *resp.Conn, args []resp.Value) bool {
	if len(args) != 3 {
		conn.WriteError(errInvalidArgsNum)
	} else {
		err := r.bitcask.Put(args[1].String(), args[2].String())
		if err != nil {
			conn.WriteError(err)
		}
		conn.WriteSimpleString("OK")
	}

	return true
}

// get implements the callback method that handles get requests.
func (r *RespServer) get(conn *resp.Conn, args []resp.Value) bool {
	if len(args) != 2 {
		conn.WriteError(errInvalidArgsNum)
	} else {
		value, err := r.bitcask.Get(args[1].String())
		if err != nil {
			conn.WriteError(err)
		} else {
			conn.WriteString(value)
		}
	}

	return true
}

// del implements the callback method that handles delete requests.
func (r *RespServer) del(conn *resp.Conn, args []resp.Value) bool {
	if len(args) != 2 {
		conn.WriteError(errInvalidArgsNum)
	} else {
		err := r.bitcask.Delete(args[1].String())
		if err != nil {
			conn.WriteError(err)
		} else {
			conn.WriteSimpleString("OK")
		}
	}

	return true
}
