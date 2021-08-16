package udp

import (
	"fmt"
	"hash/fnv"
	"net"
	"sync"

	"github.com/traefik/traefik/v2/pkg/log"
)

type serverHash struct {
	Handler
	weight int
}

// WRRLoadBalancer is a naive RoundRobin load balancer for UDP services.
type HashLoadBalancer struct {
	servers       []serverHash
	lock          sync.RWMutex
}

// NewWRRLoadBalancer creates a new WRRLoadBalancer.
func NewHashLoadBalancer() *HashLoadBalancer {
	return &HashLoadBalancer{
	}
}

// ServeUDP forwards the connection to the right service.
func (b *HashLoadBalancer) ServeUDP(conn *Conn) {
	if len(b.servers) == 0 {
		log.WithoutContext().Error("no available server")
		return
	}
	log.WithoutContext().Errorf("blue chen hash balancing udp source ip: %s", conn.rAddr.(*net.UDPAddr).IP.String())
	next, err := b.next(conn.rAddr.(*net.UDPAddr).IP.String())
	if err != nil {
		log.WithoutContext().Errorf("Error during load balancing: %v", err)
		conn.Close()
	}
	next.ServeUDP(conn)
}

// AddServer appends a handler to the existing list.
func (b *HashLoadBalancer) AddServer(serverHandler Handler) {
	w := 0
	b.servers = append(b.servers, serverHash{Handler: serverHandler, weight: w})
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (b *HashLoadBalancer) next(rAddr string) (Handler, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if len(b.servers) == 0 {
		return nil, fmt.Errorf("no servers in the pool")
	}

	idx := int(hash(rAddr) ) % len(b.servers)
	log.WithoutContext().Errorf("blue chen hash balancing find udp backend server: %v", b.servers[idx].Handler)
	return b.servers[idx], nil
}
