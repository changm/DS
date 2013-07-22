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
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "math"

type ProposeReply struct {
  ProposedNumber  int
  Accepted        bool
}

type AcceptArg struct {
  ProposalNumber  int
  Value           interface{}
}

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  peers []string
  me int // index into peers[]

  // Shoudl we split accepted / proposed?
  acceptedSequence  int
  // Interesting that interface{} is essentially void
  value             interface{}
  highestProposal   int
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
func (px *Paxos) isMajority(count int) bool {
  majority := math.Ceil( float64(len(px.peers)) / 2.0 )
  floatCount := float64(count)
  return floatCount >= majority
}

func (px *Paxos) Propose() {
  newProposal := px.highestProposal + 1
  px.SendPrepare(newProposal)
}

func (px *Paxos) Accept(args *AcceptArg, reply *ProposeReply) error {
  if args.ProposalNumber >= px.highestProposal {
    px.value = args.Value
    px.acceptedSequence = args.ProposalNumber
  } else {
    fmt.Printf("Unknown proposal number in Accept")
  }
  return nil
}

// returns true if accepted by majority
func (px *Paxos) SendPrepare(proposalNumber int) bool {
  var count = 0
  for _, peer := range px.peers {
    var reply ProposeReply
    success := call(peer, "Paxos.AcceptPrepare", proposalNumber, &reply)
    if success {
      if reply.Accepted {
        count++
      } else {
      }
    } else {
      fmt.Printf("Error calling Accept Prepare\n")
      os.Exit(1)
    }
  }

  fmt.Printf("Got count: %v\n", count)
  // Send accepts
  if px.isMajority(count) {
    acceptArg := AcceptArg{}
    acceptArg.ProposalNumber = proposalNumber
    acceptArg.Value = px.value
    var reply ProposeReply

    for _, peer := range px.peers {
      success := call(peer, "Paxos.Accept", acceptArg, &reply)
      if success {
        fmt.Printf("Accept success\n")
      } else {
        fmt.Printf("Accept failure\n");
      }
    }
  } else {
    fmt.Printf("Was not majority, exiting\n")
    os.Exit(1)
  }

  return true
}

// Returns true if it accepts this proposal
// If it does, returns highest prepare number
func (px *Paxos) AcceptPrepare(proposalNumber int, reply *ProposeReply) error {
  fmt.Printf("Proposal number is: %v. Highest proposal %v\n", proposalNumber, px.highestProposal)
  if proposalNumber > px.highestProposal {
    px.highestProposal = proposalNumber
    reply.ProposedNumber = proposalNumber
    reply.Accepted = true
  } else {
    reply.ProposedNumber = px.highestProposal
    reply.Accepted = false
  }

  fmt.Printf("Returning from accept reparep\n")
  return nil
}

// end Paxos proposal

func (px *Paxos) init() {
  px.acceptedSequence = -1
  px.highestProposal = -1
  px.value = nil
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // We just accept the first sequence and value we're given
  fmt.Printf("Sequence %v, Value is: %v\n", seq, v)
 // px.acceptedSequence = seq
  px.value = v
  px.highestProposal = seq - 1
  go px.Propose()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  fmt.Printf("Calling done\n")
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  return px.highestProposal
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
  // You code here.
  return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer's state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Only accepted if sequence number is the accepted value
  fmt.Printf("Sequence asking: %v, accepted: %v\n", seq, px.acceptedSequence)
  if (seq == px.acceptedSequence) {
    return true, px.value
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

  // Your initialization code here.

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
