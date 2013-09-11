package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "time"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "container/list"
//import "math"

type ProposeArg struct {
  ProposalNumber  int
  Value           interface{}
}

type ProposeReply struct {
  ProposalNumber  int
  Accepted        bool
}

type AcceptArg struct {
  Sequence        int
  ProposalNumber  int
  Value           interface{}
}

type AcceptReply struct {
  ProposalNumber  int
  Accepted        bool
}

type ConsensusArg struct {
  Sequence        int
  Value           interface{}
}

type ConsensusReply struct {
  Accepted        bool
}

type LeaderArg struct {
  NewLeader   string
}

type LeaderReply struct {
  Accepted    bool
}

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  peers []string
  me int // index into peers[]

  // Custom data here
  proposal    int
  value       interface{}
  maxSequence int
  minSequence int

  log map[int] interface{}
  leader  string
  work    *list.List
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(name, args, reply)
  if err == nil {
    return true
  }
  return false
}

// Begin Paxos Proposal
func (px *Paxos) isMajority(accepted map[string] bool) bool {
  var count float64 = 0
  var numOfPeers float64 = float64(len(accepted))
  for _, accepted := range accepted {
    if accepted {
      count++
    }
  }

  return (count / numOfPeers) > 0.5
}

func (px *Paxos) isLeader() bool {
  return px.peers[px.me] == px.leader
}

func (px *Paxos) AcceptPrepare(args *ProposeArg, reply *ProposeReply) error {
  if args.ProposalNumber > px.proposal {
    px.proposal = args.ProposalNumber
    reply.Accepted = true
    fmt.Printf("Me %v Accepted Proposal %v because promised propsal is: %v\n", px.me, args.ProposalNumber, px.proposal)
  } else {
    fmt.Printf("Me %v Rejected proposal %v because promised proposal is %v\n", px.me, args.ProposalNumber, px.proposal)
    reply.Accepted = false
  }

  reply.ProposalNumber = px.proposal
  return nil
}

func (px *Paxos) Propose(newProposal int, value interface{} ) bool {
  var accepted map[string] bool = make(map[string] bool)
  for _, peer := range px.peers {
    var reply ProposeReply
    var args ProposeArg
    args.ProposalNumber = newProposal
    args.Value = value

    fmt.Printf("Calling propose new proposal %v on peer %v\n", newProposal, peer)
    success := call(peer, "Paxos.AcceptPrepare", &args, &reply)
    accepted[peer] = reply.Accepted
    fmt.Printf("Peer %v Success is: %v,, reply %v\n", peer, success, reply.Accepted)
    if reply.Accepted && success {
      fmt.Printf("Accepted proposal %v\n", newProposal)
    } else {
      fmt.Printf("Me %v Rejected Proposal %v\n", px.me, newProposal)
    }
  }

  hasMajority := px.isMajority(accepted)
  //fmt.Printf("Proposal %v with value %v has majority %v\n", newProposal, value, hasMajority)
  return hasMajority
}

func (px* Paxos) isAcceptedProposal(proposal int, value interface{}) bool {
  return px.proposal == proposal
}

func (px* Paxos) AcceptRequest(args *AcceptArg, reply *AcceptReply) error {
  proposal := args.ProposalNumber
  fmt.Printf("Me %v Accept Request Proposal number is: %v\n", px.me, proposal)
  value := args.Value
  if px.isAcceptedProposal(proposal, value) {
    reply.Accepted = true
    //fmt.Printf("Commiting proposal %v to value %v\n", args.Sequence, value)
    px.commitProposal(args.Sequence, value)
  } else {
    fmt.Printf("Not accepting proposal %v, stored proposal number%v\n", proposal, px.proposal)
    reply.Accepted = false
  }

  reply.ProposalNumber = proposal
  return nil
}

func (px* Paxos) commitProposal(seq int, value interface{}) {
  //fmt.Printf("Me %v commiting sequence %v to value %v\n", px.me, seq, value)
  px.log[seq] = value
}

func (px *Paxos) Accept(seq int, proposalNumber int, value interface{} ) {
  var accepted map[string] bool = make(map[string] bool)
  for _, peer := range px.peers {
    var reply AcceptReply
    var args AcceptArg

    args.ProposalNumber = px.proposal
    args.Value = value
    args.Sequence = seq

    success := call(peer, "Paxos.AcceptRequest", &args, &reply)
    accepted[peer] = reply.Accepted
    if !reply.Accepted || !success {
      fmt.Printf("Me %v Peer %v had an error accepting\n", px.me, peer)
    }
  }

  px.commitProposal(seq, value)
}

func (px *Paxos) AppendWork(args *ConsensusArg, reply *ConsensusReply) error {
  // Probably ahve to grab a mutex for this
  px.work.PushBack(args)
  return nil
}

func (px *Paxos) doWork() {
  for {
    if px.work.Len() > 0 {
      work := px.work.Front().Value.(*ConsensusArg)
      px.work.Remove(px.work.Front())
      var reply ConsensusReply
      px.Consensus(work, &reply)
    } else {
      time.Sleep(time.Second)
    }
  }
}

func (px *Paxos) Consensus(args *ConsensusArg, reply *ConsensusReply) error {
  // TODO: Replace sequence number with proposal number
  seq := args.Sequence
  decided, _ := px.Status(seq)
  if decided {
    return nil
  }

  value := args.Value
  proposalNumber := px.proposal + 1
  //fmt.Printf("Me %v starting Sequence %v with value %v\n", px.me, seq, value)
  acceptedPropose := px.Propose(proposalNumber, value)
  if acceptedPropose {
    px.Accept(seq, proposalNumber, value)
  }

  return nil
}

func (px *Paxos) NewLeader(arg *LeaderArg, reply *LeaderReply) error {
  px.leader = arg.NewLeader
  reply.Accepted = true
  fmt.Printf("Me %v made leader %v\n", px.me, px.leader)
  if px.peers[px.me] == px.leader {
    go px.doWork()
  }
  return nil
}

func (px *Paxos) broadcastLeader(newLeader string) {
  for _, peer := range px.peers {
    var arg LeaderArg
    var reply LeaderReply
    arg.NewLeader = newLeader

    success := call(peer, "Paxos.NewLeader", &arg, &reply)
    if success && reply.Accepted {
      fmt.Printf("Peer %v Made leader %v\n", peer, newLeader)
    } else {
      fmt.Printf("Error, peer %v did not get leader notification. success is %v, reply is %v\n", peer, success, reply.Accepted)
    }
  }

  px.leader = newLeader
  fmt.Printf("FInished broadcasting leader\n")
}

func (px* Paxos) Ping(args *int, reply *int) error {
  fmt.Printf("Getting a ping %v\n", px.me)
  return nil
}

func (px *Paxos) initListener() {
    rpcs := rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(px.peers[px.me]) // only needed for "unix"
    l, e := net.Listen("unix", px.peers[px.me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l

    // We died because we removed the the RPC 
    // Reinitialize it, couldn't find a cleaner way to do this
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
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
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", px.me, err.Error())
          px.Kill()
        }
      }
    }()
}

func (px *Paxos) GetLeader(args *int, reply *string) error {
  *reply = px.leader
  return nil
}

func (px *Paxos) askNeighborForLeader() bool {
  for _, peer := range px.peers {
    if peer == px.peers[px.me] {
      continue
    }

    var args = 0
    var newLeader string
    success := call (peer, "Paxos.GetLeader", &args, &newLeader)
    if success && newLeader != "" && newLeader != px.leader {
      fmt.Printf("Me %v Got leader from peer %v, is %v\n", px.me, peer, newLeader)
      px.leader = newLeader
      return true
    }
  }

  return false
}

func (px *Paxos) electLeader() bool {
  if px.askNeighborForLeader() {
    return true
  }

  for _, peer := range px.peers {
    var args = 0
    var reply = 0

    alive := call(peer, "Paxos.Ping", &args, &reply)
    if alive {
      px.broadcastLeader(peer)
      return true
    }
  }

  return false
}

// end Paxos proposal

func (px *Paxos) reset() {
  px.init()
}

func (px *Paxos) init() {
  px.proposal = 0
  px.value = nil
  px.log = make(map[int] interface{})
  px.maxSequence = -1
  px.minSequence = -1
  if px.me == 0 {
    px.electLeader()
  }

  px.work = list.New()
}

func (px *Paxos) ensureAlive() {
  var args = 0
  var reply = 0

  alive := call(px.peers[px.me], "Paxos.Ping", &args, &reply)
  if !alive {
    px.initListener()
    px.askNeighborForLeader()
    fmt.Printf("I brought %v up again, leader is %v\n", px.me, px.leader)
  }
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  //fmt.Printf("Me %v Starting %v, Value is: %v\n", px.me, seq, v)
  var args ConsensusArg;
  var reply ConsensusReply;
  args.Sequence = seq
  args.Value = v

  if seq > px.maxSequence {
    px.maxSequence = seq
  }

  // If we died, make sure we're up again
  px.ensureAlive()
  var success = call(px.leader, "Paxos.AppendWork", &args, &reply)
  for !success {
    px.electLeader()
    fmt.Printf("Made new leader, trying to append work\n")
    success = call(px.leader, "Paxos.AppendWork", &args, &reply)
    fmt.Printf("Trying again with new leader\n")
  }

  fmt.Printf("Returning from start\n")
}

// For now, just tell the leader to fetch the minimum
func (px *Paxos) UpdateMin(seq int) {

}

func (px *Paxos) GetMin(args *int, reply *int) error {
  *reply = px.minSequence
  return nil
}

func (px* Paxos) GetPaxosMin(args *int, reply *int) error {
  var callArgs int
  var callReply int
  var totalMin = 100000
  for _, peer := range px.peers {
    call(peer, "Paxos.GetMin", &callArgs, &callReply)
    if callReply < totalMin {
      totalMin = callReply
    }
  }

  *reply = totalMin
  return nil
}

func (px* Paxos) RequestPaxosMin() int {
  var reply int
  var args int
  success := call(px.leader, "Paxos.GetPaxosMin", &args, &reply)
  if !success {
    fmt.Printf("Error Calling Leader for Paxos Min. Leader is %v\n", px.leader)
    return -1
  }

  fmt.Printf("Requested Paxos Min, got %v\n", reply)
  return reply
}

// Ask all peers for their min
func (px* Paxos) updateMin(seq int) {
  px.minSequence = seq
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // lookup for loop syntax
  for i:= 0; i <= seq; i++ {
    delete(px.log, i)
  }

  fmt.Printf("Called Done peer %v on sequence %v\n", px.me, seq)
  px.updateMin(seq)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  return px.maxSequence
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peer's z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// It is illegal to call Done(i) on a peer and
// then call Start(j) on that peer for any j <= i.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peer's Min doesn't reflect another Peer's Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() can't increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers' Min()s won't increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  val := px.RequestPaxosMin() + 1
  fmt.Printf("Fetched min on %v - Return %v\n", px.me, val)
  return val
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer's state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  value, valid := px.log[seq]
  if valid {
    return true, value
  }

  return false, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this server's port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me
  px.init()

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
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
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
          px.Kill()
        }
      }
    }()
  }


  return px
}
