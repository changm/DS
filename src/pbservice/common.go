package pbservice

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
  ErrBackupFail = "Err No Backup Update"
  ErrDiffValue = "Error. Backup and Primary have different values"
)
type Err string

type PutArgs struct {
  Key string
  Value string
}

type PutReply struct {
  Err Err
}

type GetArgs struct {
  Key string
}

type GetReply struct {
  Err Err
  Value string
}

type ImageArgs struct {
}

// Your RPC definitions here.
