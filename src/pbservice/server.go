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


type PBServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  currentView uint
  isPrimary bool
  isBackup bool
  values map[string] string
  // Your declarations here.
}

func (pb *PBServer) init() {
  pb.currentView = 0
  pb.isPrimary = false
  pb.isBackup = false
  pb.values = make(map[string] string)
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  key := args.Key
  if pb.isPrimary {
    value, validKey := pb.values[key]
    if validKey {
      reply.Value = value
    } else {
      reply.Value = ""
      reply.Err = ErrNoKey
    }
  } else {
    reply.Err = ErrWrongServer
  }

  return nil
}

func (pb *PBServer) updateBackup(args *PutArgs) error {
  // TODO update backup
  fmt.Printf("Updating backup\n");
  return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  key, value := args.Key, args.Value
  fmt.Printf("PUTTING SOMETHING\n")
  if pb.isPrimary {
    fmt.Printf("PB is primary\n")
    pb.values[key] = value
    pb.updateBackup(args)
    reply.Err = OK
  } else {
    reply.Err = ErrWrongServer
  }
  return nil
}

func (pb *PBServer) transitionView(view viewservice.View) {
  if pb.me == view.Primary {
    pb.isPrimary = true
    pb.isBackup = false
  } else if pb.me == view.Backup {
    pb.isPrimary = false
    pb.isBackup = true
  } else {
    fmt.Printf("PB Server transitioned view but not backup or primary\n")
  }

  pb.currentView = view.Viewnum
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
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
  fmt.Printf("View service: %v\n", pb.vs)
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
