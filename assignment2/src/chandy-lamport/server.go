package chandy_lamport

import (
	"log"
	"strconv"
	"fmt"
)

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE
	snapshots     map[int]*SnapshotState //Asked in the forums good hint :D [snapshotId][snapshot]
	messages map[string]int //[message][0 = close 1 = open] boolean also works
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		make(map[int]*SnapshotState),
		make(map[string]int),
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME

	//Which message is it?
	switch message := message.(type) {
		//is it marker?
		case MarkerMessage:
			//gets the snapshotid
			snapshotId := message.snapshotId

			//RECEIVE RULE
			//if not yet snapshotted as it says in Mahdi presentation
			_, snapshoted := server.snapshots[snapshotId]

			if !snapshoted {
				//if not then snapshot it!
				server.StartSnapshot(snapshotId)
				//NotifySnapshotComplete BUT, simulator.go injectEvent calls also StartSnapshot, then moved to startSnapShot
			}


			//FROM RECIEVING CHANNELS THEN RECORD AS EMPTY

			// Close the saving of messages for that snapshot (BUT only for that channel)  // 0_N1, 0_N2, 1_N1, 2_N1 ,3_N1 ,0_N3,1_N2 
			composedKey := src + "_" + strconv.Itoa(snapshotId)
			server.messages[composedKey] = 0
			

		case TokenMessage:

			// Adds up incoming tokens
			server.Tokens += message.numTokens

			for _, currentSnapshot := range server.snapshots {
				fmt.Println("********************"+src+"||||"+strconv.Itoa(currentSnapshot.id))
				// Add the messages to each one of the snapshots that are open (Those that are not in the messages map)
				composedKey := src + "_" + strconv.Itoa(currentSnapshot.id)
				if _, f := server.messages[composedKey]; !f {
					snapshotMessage := SnapshotMessage{src, server.Id, message}
					currentSnapshot.messages = append(currentSnapshot.messages, &snapshotMessage)
				}
			}
	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME
	
	// Build the tokens map (Even if it will only have this server tokens)
	serverTokens := make(map[string]int)
	serverTokens[server.Id] = server.Tokens

	// Build the messages array
	serverMessages := make([]*SnapshotMessage, 0)

	// Store the own snapshot in the server snapshots map
	server.snapshots[snapshotId] = &SnapshotState{snapshotId, serverTokens, serverMessages}

	// Send a marker message to all the server channels
	server.SendToNeighbors(MarkerMessage{snapshotId})
	// Notify
	server.sim.NotifySnapshotComplete(server.Id, snapshotId)
}
