package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  listener net.Listener
  dead bool
  me string

  // Your declarations here.
  currentView   *View
  nextView      *View

  primaryAck    bool
  backupAck     bool
  timestamps    map[string] time.Time
}

func printView(view *View) {
  fmt.Printf("View %v - Primary %v - Backup %v\n", view.Viewnum, view.Primary, view.Backup)
}

func (vs* ViewServer) updateTimestamp(server string) {
  vs.timestamps[server] = time.Now()
}

func (vs *ViewServer) cloneCurrentView() *View {
  return &View{vs.currentView.Viewnum, vs.currentView.Primary, vs.currentView.Backup}
}

func (vs *ViewServer) addServer(server string) {
  var currentView = vs.getCurrentView()
  if currentView.Primary == "" {
    vs.nextView = &View{vs.currentView.Viewnum, server, ""}
    vs.switchToNextView()
  } else if currentView.Backup == "" && !vs.isPrimary(server) {
    vs.nextView = vs.cloneCurrentView()
    vs.nextView.Backup = server
    vs.switchToNextView()
    vs.updateTimestamp(server)
  }
}

func (vs *ViewServer) removeServer(server string) {
  //fmt.Printf("Removing server %v\n", server)
  //printView(vs.currentView)
  if vs.isPrimary(server) {
    //fmt.Printf("REmoving primary\n")
    vs.nextView = vs.cloneCurrentView()
    vs.nextView.Primary = vs.nextView.Backup
    vs.nextView.Backup = ""
    vs.switchToNextView()
  } else if vs.isBackup(server) {
    //fmt.Printf("Removing backup\n")
    vs.nextView = vs.cloneCurrentView()
    vs.nextView.Backup = ""
    vs.switchToNextView()
  } else {
    fmt.Printf("Unknown server died from tick %v\n", server)
    delete(vs.timestamps, server)
  }
}



func (vs* ViewServer) isSameView(viewNumber uint) bool {
  return vs.getCurrentView().Viewnum == viewNumber
}

func (vs* ViewServer) getCurrentView() *View {
  return vs.currentView
}

func (vs* ViewServer) isPrimary(server string) bool {
  return vs.currentView.Primary == server ||
    vs.currentView.Primary == ""
}

func (vs* ViewServer) isBackup(server string) bool {
  return vs.currentView.Backup == server
}

func (vs* ViewServer) ackView(args* PingArgs) {
  server := args.Me
  view := args.Viewnum
  //fmt.Printf("Checking ack view %v # %v\n", server, view)
  //printView(vs.currentView)
  if vs.isPrimary(server) && vs.isSameView(view) {
    vs.primaryAck = true
    //fmt.Printf("Ack view %v # %v- %v\n", server, view, vs.primaryAck)
  } else if vs.isBackup(server) && vs.isSameView(view) {
    vs.backupAck = true
  }

  if vs.isSameView(view) {
    //fmt.Printf("Updating timestamp server %v view %v\n", server, view)
    vs.updateTimestamp(server)
  }
}

func (vs* ViewServer) inSync() bool {
  return vs.primaryAck && vs.backupAck
}

func (vs* ViewServer) hasOnlyPrimary() bool {
  return vs.currentView.Backup == "" && vs.primaryAck
}

func (vs* ViewServer) switchToNextView() bool {
  if vs.inSync() || vs.hasOnlyPrimary() {
    vs.nextView.Viewnum = vs.currentView.Viewnum + 1
    vs.currentView = vs.nextView
    vs.primaryAck = false
    vs.backupAck = false
    return true
  }

  return false;
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  vs.mu.Lock()
  defer vs.mu.Unlock()
  vs.ackView(args)
  vs.addServer(args.Me)
  reply.View = *(vs.getCurrentView())
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
  reply.View = *(vs.getCurrentView())
  return nil
}

func (vs* ViewServer) deadServer(server string, timestamp time.Time) bool {
  currentTime := time.Now()
  deadline := timestamp.Add(PingInterval * DeadPings)
  //fmt.Printf("Checking server %v, timestamp %v\n", server, timestamp)
  return currentTime.After(deadline)
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
  vs.mu.Lock()
  defer vs.mu.Unlock()

  for server, timestamp := range vs.timestamps {
    if vs.deadServer(server, timestamp) {
      //fmt.Printf("Removing server %v\n", server)
      vs.removeServer(server)
    }
  }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.listener.Close()
}

func (vs* ViewServer) init(nameString string) {
  vs.me = nameString
  vs.dead = false
  vs.currentView = &View{0, "", ""}
  vs.timestamps = make(map[string]time.Time)
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.init(me);

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.listener = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.listener.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
