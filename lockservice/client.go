package lockservice

//
// the lockservice Clerk lives in the client
// and maintains a little state.
//
type Clerk struct {
  servers [2]string // primary port, backup port
  // Your definitions here.
}

func MakeClerk(primary string, backup string) *Clerk {
  ck := new(Clerk)
  ck.servers[0] = primary
  ck.servers[1] = backup
  // Your initialization code here.
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
  args := &LockArgs{}
  args.Lockname = lockname
  var reply LockReply
  
  // send an RPC request, wait for the reply.
  ok := call(ck.servers[0], "LockServer.Lock", args, &reply)
  if ok == false {

  // Talk to the backup
    ok := call(ck.servers[1], "LockServer.Lock", args, &reply)
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
  // prepare the arguments.
  args := &UnlockArgs{}
  args.Lockname = lockname
  var reply UnlockReply
  
  // send an RPC request, wait for the reply.
  ok := call(ck.servers[0], "LockServer.Unlock", args, &reply)
  if ok == false {

    ok := call(ck.servers[1], "LockServer.Unlock", args, &reply)
    if ok == false {
      return false
    }
  }
  
  return reply.OK
}
