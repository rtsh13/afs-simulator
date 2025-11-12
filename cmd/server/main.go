package main

import (
	"flag"
	"log"
	"strings"

	afs "github.com/afs-simulator/pkg/afs"
)

func main() {
	workingDir := flag.String("working", "data", "Working directory path")
	address := flag.String("addr", ":8080", "Server address")

	id := flag.Int("id", 0, "Unique Server ID")
	replicas := flag.String("replicas", "", "Comma-separated list of ALL replica addresses (including self)")
	isPrimaryFlag := flag.Bool("primary", false, "Start this server as the initial primary")

	flag.Parse()

	// string commas to get the list of replicas list
	replicaAddrs := []string{}
	if *replicas != "" {
		parts := strings.Split(*replicas, ",")
		for _, addr := range parts {
			trimmed := strings.TrimSpace(addr)
			if trimmed != "" && trimmed != strings.TrimSpace(*address) {
				replicaAddrs = append(replicaAddrs, trimmed)
			}
		}
	}

	log.Printf("Starting replica server %d on %s", *id, *address)
	log.Printf("Will connect to replicas: %v", replicaAddrs)

	server, err := afs.NewReplicaServer(*id, *workingDir, replicaAddrs)
	if err != nil {
		log.Fatalf("Failed to create replica server: %v", err)
	}

	if *isPrimaryFlag {
		log.Printf("Server %d starting as PRIMARY", *id)
		server.BecomePrimary()
	} else {
		log.Printf("Server %d starting as BACKUP", *id)
	}

	log.Fatal(server.Start(*address))
}
