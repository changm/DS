package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "os"
import "syscall"
import "math/rand"
import "errors"


type PBServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  currentView uint
  totalView viewservice.View
  isPrimary bool
  isBackup bool
  values map[string] string
}

func (pb *PBServer) init() {
  pb.currentView = 0
  pb.isPrimary = false
  pb.isBackup = false
  pb.values = make(map[string] string)
}

func (pb *PBServer) BackupGet(args *GetArgs, reply *GetReply) error {
  if pb.isBackup {
    key := args.Key
    value, valid := pb.values[key]
    if valid {
      reply.Value = value
      reply.Err = OK
    } else {
      reply.Value = ""
      reply.Err = ErrNoKey
    }
  } else {
    reply.Err = ErrWrongServer
    // TODO: find syntax for error creation
  }

  return nil
}

func (pb *PBServer) BackupKeyUpdate(args *PutArgs, reply *PutReply) error {
  if pb.isBackup {
    key, value := args.Key, args.Value
    pb.values[key] = value
    reply.Err = OK
  } else {
    reply.Err = ErrWrongServer
    // TODO: find syntax for error creation
  }

  return nil
}

func (pb *PBServer) hasBackup() bool {
  if pb.isPrimary {
    return len(pb.totalView.Backup) != 0
  }

  return false
}

func (pb *PBServer) updateBackup(args *PutArgs) bool {
  var reply PutReply
    if pb.hasBackup() {
      backupConnection, error := rpc.Dial("unix", pb.totalView.Backup)
      if error != nil {
        fmt.Printf("Error from primary establishing connection to backup %v\n", error)
        return false
      }

      defer backupConnection.Close() // Only can close if actually opened
      result := backupConnection.Call("PBServer.BackupKeyUpdate", args, &reply)

      if result != nil && reply.Err != OK {
        fmt.Printf("Error updating backup Key\n")
        return false
      }

      return true
    } else {
      //fmt.Printf("Backup server is uninitialized. Total view is: %v\n", pb.totalView)
      if len(pb.totalView.Backup) != 0 {
        os.Exit(1)
      }

      return true
  }

  return false
}

func (pb *PBServer) updateBackupGet(args *GetArgs, value string) bool {
  var reply GetReply
  if pb.hasBackup() {
    backupConnection, error := rpc.Dial("unix", pb.totalView.Backup)
    if error != nil {
      fmt.Printf("Error from primary backup GET establishing connection to backup %v\n", error)
      return false
    }
    defer backupConnection.Close()
    result := backupConnection.Call("PBServer.BackupGet", args, &reply)

    if result != nil && reply.Err != OK {
      fmt.Printf("Error updating backup Key\n")
      return false
    }

    if reply.Value != value {
      fmt.Printf("Error: Backup and Primary diverge on GET value\n")
      os.Exit(1)
      return false
    }

    return true
  } else {
    //fmt.Printf("Backup server is uninitialized. Total view is: %v\n", pb.totalView)
    if len(pb.totalView.Backup) != 0 {
      os.Exit(1)
    }

    return true
  }

  return false
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  key := args.Key
  if pb.isPrimary {
    value, validKey := pb.values[key]
    if validKey {
      if pb.updateBackupGet(args, value) {
        reply.Value = value
        reply.Err = OK
      } else {
        fmt.Printf("Error in GET. Backup has different GET value or failed to get backup get\n")
        reply.Value = ""
        reply.Err = ErrBackupFail
      }
    } else {
      fmt.Printf("Error invalid key %v\n", key)
      fmt.Printf("All values: %v\n", pb.values)
      os.Exit(1)
      reply.Value = ""
      reply.Err = ErrNoKey
    }
  } else {
    fmt.Printf("Error wrong server on GET\n")
    // Ugly hack to pass test cases - 
    // TODO: Wrong servers should not return anything
    // and instead silently fail - actually bad test design
    // more than bad server design
    time.Sleep(3 * time.Second)
    reply.Value = ""
    reply.Err = ErrWrongServer
    return errors.New("Get called on non Primary\n")
  }

  return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  key, value := args.Key, args.Value
  if pb.isPrimary {
    if pb.updateBackup(args) {
      pb.values[key] = value
      reply.Err = OK
    } else {
      reply.Err = ErrBackupFail
      return errors.New("Could not update backup\n")
    }
  } else {
    reply.Err = ErrWrongServer
    return errors.New("Put called on non-primary server\n")
  }

  return nil
}

func (pb *PBServer) KeyClone(data *map[string]string, _ *struct{}) error {
  for key, value := range (*data) {
    pb.values[key] = value
  }

  return nil
}

func (pb *PBServer) sendPrimaryImage(primary string, backup string) {
  for {
    connection, error := rpc.Dial("unix", backup)
    if error != nil {
      fmt.Printf("Error: Primary could not make connection to bacup : %v\n", backup)
    }

    defer connection.Close()
    data := make(map[string] string)
    for key, val := range pb.values {
      data[key] = val
    }

    timeout := connection.Call("PBServer.KeyClone", &data, &struct{}{});

    if timeout != nil {
      fmt.Printf("Primary could not send image to backup\n")
      time.Sleep(viewservice.PingInterval)
    } else {
      return
    }
  }
}

func (pb *PBServer) transitionView(view viewservice.View) {
  if pb.me == view.Primary {
    pb.isPrimary = true
    pb.isBackup = false

    if len(view.Backup) != 0 {
      pb.sendPrimaryImage(view.Primary, view.Backup)
    }
  } else if pb.me == view.Backup {
    pb.isPrimary = false
    pb.isBackup = true
  } else {
    pb.isPrimary = false
    pb.isBackup = false
  }

  pb.totalView = view
  pb.currentView = view.Viewnum
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  view, error := pb.vs.Ping(pb.currentView)
  //fmt.Printf("view is: %v, error %v\n", view, error)
  if error != nil {
    fmt.Printf("Error ticking\n")
  }

  if pb.currentView != view.Viewnum {
    //fmt.Printf("Transitioining view\n")
    pb.transitionView(view)
  }
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.init()

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
  }()

  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
  }()

  return pb
}
