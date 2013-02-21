package lockservice

import "net"
import "net/rpc"
import "log"
import "sync"
import "fmt"
import "os"
import "io"
import "time"

type LockServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool  // for test_test.go
  dying bool // for test_test.go

  am_primary bool // am I the primary?
  backup string   // backup's port

  // for each lock name, is it locked?
  locks map[string]bool
  locks_time map[string]time.Time
}

func isDuplicateMessage(lockServer *LockServer, args *LockArgs) bool {
  return lockServer.locks_time[args.Lockname] == args.Stamp;
}

func isDupUnlock(lockServer *LockServer, args *UnlockArgs) bool {
  return lockServer.locks_time[args.Lockname] == args.Stamp;
}


//
// server Lock RPC handler.
//
// you will have to modify this function
//
func (ls *LockServer) Lock(args *LockArgs, reply *LockReply) error {
  ls.mu.Lock()
  // Actually only have 1 global lock
  defer ls.mu.Unlock()

  locked, _ := ls.locks[args.Lockname]

/*
  if !ls.am_primary {
    fmt.Printf("Backup Is locking - %v. Already locked %v\n", args.Lockname, locked);
  } else {
    fmt.Printf("Primary is locking %v. Already locked  %v\n", args.Lockname, locked);
  }
  */

  if locked {
    if (isDuplicateMessage(ls, args)) {
      //fmt.Printf("Is duplicate\n");
      reply.OK = true;
      } else {
      reply.OK = false
      }
  } else {
    reply.OK = true
    ls.locks[args.Lockname] = true
    ls.locks_time[args.Lockname] = args.Stamp
  }

  if ls.am_primary {
    //var backupReply LockReply
    //fmt.Printf("Primary updating backup\n");
    call (ls.backup, "LockServer.Lock", args, reply)
    //fmt.Printf("Primary returning %v\n", reply.OK);
  }

  return nil
}

func (ls *LockServer) IsLocked(args *LockArgs, reply *LockReply) error {
  reply.OK = ls.locks[args.Lockname];
  return nil
}

//
// server Unlock RPC handler.
//
func (ls *LockServer) Unlock(args *UnlockArgs, reply *UnlockReply) error {
  var locked, _ = ls.locks[args.Lockname];
  //fmt.Printf("CALLIMG UNLOCK\n");

/*
  if !ls.am_primary {
    fmt.Printf("Backup Is unlocking, current lock %v - %v\n", args.Lockname, locked);
  } else {
    fmt.Printf("Unlocking now\n");
  }
  */

  if locked {
    ls.mu.Lock();
    ls.mu.Unlock();

    reply.OK = true;
    ls.locks[args.Lockname] = false
    ls.locks_time[args.Lockname] = args.Stamp
  } else {
    if (isDupUnlock(ls, args)) {
      reply.OK = true;
    } else {
      reply.OK = false;
    }
  }

  if ls.am_primary {
    var backupReply LockReply
      call (ls.backup, "LockServer.Unlock", args, &backupReply);
    // TODO check backup
  }

  return nil
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this.
//
func (ls *LockServer) kill() {
  ls.dead = true
  ls.l.Close()
}

//
// hack to allow test_test.go to have primary process
// an RPC but not send a reply. can't use the shutdown()
// trick b/c that causes client to immediately get an
// error and send to backup before primary does.
// please don't change anything to do with DeafConn.
//
type DeafConn struct {
  c io.ReadWriteCloser
}
func (dc DeafConn) Write(p []byte) (n int, err error) {
  return len(p), nil
}
func (dc DeafConn) Close() error {
  return dc.c.Close()
}
func (dc DeafConn) Read(p []byte) (n int, err error) {
  return dc.c.Read(p)
}

func initLockServer(primary string, backup string, am_primary bool) *LockServer {
  ls := new(LockServer)
  ls.backup = backup
  ls.am_primary = am_primary
  ls.locks = map[string]bool{}
  ls.locks_time = map[string]time.Time{}
  return ls;
}

func StartServer(primary string, backup string, am_primary bool) *LockServer {
  ls := initLockServer(primary, backup, am_primary);

  // Your initialization code here.


  me := ""
  if am_primary {
    me = primary
  } else {
    me = backup
  }

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(ls)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(me) // only needed for "unix"
  l, e := net.Listen("unix", me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  ls.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for ls.dead == false {
      conn, err := ls.l.Accept()
      if err == nil && ls.dead == false {
        if ls.dying {
          // process the request but force discard of reply.

          // without this the connection is never closed,
          // b/c ServeConn() is waiting for more requests.
          // test_test.go depends on this two seconds.
          go func() {
            time.Sleep(2 * time.Second)
            conn.Close()
          }()
          ls.l.Close()

          // this object has the type ServeConn expects,
          // but discards writes (i.e. discards the RPC reply).
          deaf_conn := DeafConn{c : conn}

          rpcs.ServeConn(deaf_conn)

          ls.dead = true
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && ls.dead == false {
        fmt.Printf("LockServer(%v) accept: %v\n", me, err.Error())
        ls.kill()
      }
    }
  }()

  return ls
}
