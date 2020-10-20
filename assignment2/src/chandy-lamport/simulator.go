package chandy_lamport

import (
	"log"
	"math/rand"
)

// Max random delay added to packet delivery
const maxDelay = 5

// Simulator is the entry point to the distributed snapshot application.
//
// It is a discrete time simulator, i.e. events that happen at time t + 1 come
// strictly after events that happen at time t. At each time step, the simulator
// examines messages queued up across all the links in the system and decides
// which ones to deliver to the destination.
//
// The simulator is responsible for starting the snapshot process, inducing servers
// to pass tokens to each other, and collecting the snapshot state after the process
// has terminated.
type Simulator struct {
	time           int
	nextSnapshotId int
	servers        map[string]*Server // key = server ID
	logger         *Logger
	// TODO: ADD MORE FIELDS HERE
	availableChannels       map[int]chan string
}

func NewSimulator() *Simulator {
	return &Simulator{
		0,
		0,
		make(map[string]*Server),
		NewLogger(),
		make(map[int]chan string),
	}
}

// Return the receive time of a message after adding a random delay.
// Note: since we only deliver one message to a given server at each time step,
// the message may be received *after* the time step returned in this function.
func (sim *Simulator) GetReceiveTime() int {
	return sim.time + 1 + rand.Intn(5)
}

// Add a server to this simulator with the specified number of starting tokens
func (sim *Simulator) AddServer(id string, tokens int) {
	server := NewServer(id, tokens, sim)
	sim.servers[id] = server
}

// Add a unidirectional link between two servers
func (sim *Simulator) AddForwardLink(src string, dest string) {
	server1, ok1 := sim.servers[src]
	server2, ok2 := sim.servers[dest]
	if !ok1 {
		log.Fatalf("Server %v does not exist\n", src)
	}
	if !ok2 {
		log.Fatalf("Server %v does not exist\n", dest)
	}
	server1.AddOutboundLink(server2)
}

// Run an event in the system
func (sim *Simulator) InjectEvent(event interface{}) {
	switch event := event.(type) {
	case PassTokenEvent:
		src := sim.servers[event.src]
		src.SendTokens(event.tokens, event.dest)
	case SnapshotEvent:
		sim.StartSnapshot(event.serverId)
	default:
		log.Fatal("Error unknown event: ", event)
	}
}

// Advance the simulator time forward by one step, handling all send message events
// that expire at the new time step, if any.
func (sim *Simulator) Tick() {
	sim.time++
	sim.logger.NewEpoch()
	// Note: to ensure deterministic ordering of packet delivery across the servers,
	// we must also iterate through the servers and the links in a deterministic way
	for _, serverId := range getSortedKeys(sim.servers) {
		server := sim.servers[serverId]
		for _, dest := range getSortedKeys(server.outboundLinks) {
			link := server.outboundLinks[dest]
			// Deliver at most one packet per server at each time step to
			// establish total ordering of packet delivery to each server
			if !link.events.Empty() {
				e := link.events.Peek().(SendMessageEvent)
				if e.receiveTime <= sim.time {
					link.events.Pop()
					sim.logger.RecordEvent(
						sim.servers[e.dest],
						ReceivedMessageEvent{e.src, e.dest, e.message})
					sim.servers[e.dest].HandlePacket(e.src, e.message)
					break
				}
			}
		}
	}
}

// Start a new snapshot process at the specified server
func (sim *Simulator) StartSnapshot(serverId string) {
	snapshotId := sim.nextSnapshotId
	sim.nextSnapshotId++ //0 ,1 ,2 ,3  {N1, N2, N3}
	sim.logger.RecordEvent(sim.servers[serverId], StartSnapshot{serverId, snapshotId})

	// TODO: IMPLEMENT ME

	// Get current node/server and create snapshot
	currentServer := sim.servers[serverId]
	currentServer.StartSnapshot(snapshotId)


}

// Callback for servers to notify the simulator that the snapshot process has
// completed on a particular server
func (sim *Simulator) NotifySnapshotComplete(serverId string, snapshotId int) {
	sim.logger.RecordEvent(sim.servers[serverId], EndSnapshot{serverId, snapshotId})
	// TODO: IMPLEMENT ME

	// If there is no channel yet for this snapshot, create it
	_, found := sim.availableChannels[snapshotId]
	if !found {
		sim.availableChannels[snapshotId] = make(chan string, 20)
	}

	// blocking channel
	sim.availableChannels[snapshotId] <- serverId
}

// CollectSnapshot collects and merges snapshot state from all the servers.
// This function blocks until the snapshot process has completed on all servers.
func (sim *Simulator) CollectSnapshot(snapshotId int) *SnapshotState {
	// TODO: IMPLEMENT ME
	snap := SnapshotState{snapshotId, make(map[string]int), make([]*SnapshotMessage, 0)}

	// Make a map of the servers that are pending to complete their snapshot
	// (Initially, all the servers are pending)
	pendingServers := make(map[string]bool)
	for key := range sim.servers {
		pendingServers[key] = true
	}

	//removing all pendings
	for len(pendingServers) > 0 {
		completedServerID := <-sim.availableChannels[snapshotId]
		delete(pendingServers, completedServerID)
	}

	for _, server := range sim.servers {
		// Get each server snapshot
		serverSnapshot := server.snapshots[snapshotId]

		// Global snapshot
		for key, value := range serverSnapshot.tokens {
			snap.tokens[key] = value
		}

		// appends the messages too
		for _, value := range serverSnapshot.messages {
			snap.messages = append(snap.messages, value)
		}
	}

	return &snap
}