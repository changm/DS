package lockservice
//import "fmt"
import "sync"

//
// the lockservice Clerk lives in the client
// and maintains a little state.
//
type Clerk struct {
  servers [2]string // primary port, backup port
}

func MakeClerk(primary string, backup string) *Clerk {
  ck := new(Clerk)
  ck.servers[0] = primary
  ck.servers[1] = backup
  return ck
}

var id int;
var clockLock sync.Mutex;

func getId() int {
  clockLock.Lock()
  defer clockLock.Unlock()
  id++
  return id
}

//
// ask the lock service for a lock.
// returns true if the lock service
// granted the lock, false otherwise.
//
// you will have to modify this function.
//
func (ck *Clerk) Lock(lockname string) bool {
  return ck.sendRequest(lockname, "LockServer.Lock")
}

//
// ask the lock service to unlock a lock.
// returns true if the lock was previously held,
// false otherwise.
//
func (ck *Clerk) Unlock(lockname string) bool {
  return ck.sendRequest(lockname, "LockServer.Unlock")
}

func (ck *Clerk) sendRequest(lockname string, service string) bool {
  args := &LockArgs{}
  args.Lockname = lockname
  args.Id = getId()
  var reply LockReply

  // send an RPC request, wait for the reply.
  isAlive := call(ck.servers[0], service, args, &reply)
  if isAlive == false {
    return ck.recover(args, service)
  }

  return reply.OK
}

// When the primary dies, swap to the backup
func (ck *Clerk) switchPrimary() bool {
  ck.servers[0] = ck.servers[1]
  args := &LockArgs{}
  var reply LockReply

  call(ck.servers[0], "LockServer.BecomePrimary", args, &reply)
  ck.servers[1] = "Undefined"
  return true
}

func (ck *Clerk) recover(args* LockArgs, service string) bool {
  ck.switchPrimary()
  var reply LockReply
  call(ck.servers[0], "LockServer.GetLog", args, &reply)
  if reply.LogEntry {
    return reply.OK
  }

  //fmt.Printf("Didn't have a log so submitting actual request\n")
  call(ck.servers[0], service, args, &reply)
  return reply.OK
}
