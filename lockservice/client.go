package lockservice
//import "fmt"

var clock int;

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


//
// ask the lock service for a lock.
// returns true if the lock service
// granted the lock, false otherwise.
//
// you will have to modify this function.
//
func (ck *Clerk) Lock(lockname string) bool {
  // prepare the arguments.
  clock++;

  args := &LockArgs{}
  args.Lockname = lockname
  args.Clock = clock

  var reply LockReply
  //fmt.Printf("Sending primary lock %v\n", args.Stamp);
  // send an RPC request, wait for the reply.
  isAlive := call(ck.servers[0], "LockServer.Lock", args, &reply)
  if isAlive == false {
    // Talk to the backup
    //fmt.Printf("\nPrimary failed. Sending backup lock %v\n", args.Clock);
    ok := call(ck.servers[1], "LockServer.Lock", args, &reply)
    //fmt.Printf("Calling backup %v\n", reply.OK);

    if ok == false {
      return false
    }
  }

  return reply.OK
}


//
// ask the lock service to unlock a lock.
// returns true if the lock was previously held,
// false otherwise.
//
func (ck *Clerk) Unlock(lockname string) bool {
  clock++;

  // prepare the arguments.
  args := &UnlockArgs{}
  args.Lockname = lockname
  args.Clock = clock

  var reply UnlockReply

  // send an RPC request, wait for the reply.
  isAlive := call(ck.servers[0], "LockServer.Unlock", args, &reply)
  if isAlive == false {
    ok := call(ck.servers[1], "LockServer.Unlock", args, &reply)
    if ok == false {
      return false
    }
  }

  return reply.OK
}
