package main

import (
	"flag"
	"log"
	"strings"

	afs "github.com/afs-simulator/pkg/afs"
)

func main() {
	inputDir := flag.String("input", "data/input", "Input directory path")
	outputDir := flag.String("output", "data/output", "Output directory path")
	address := flag.String("addr", ":8080", "Server address")

	id := flag.String("id", "server-default", "Unique Server ID")
	replicas := flag.String("replicas", "", "Comma-separated list of OTHER replica addresses (exclude self)")
	isPrimaryFlag := flag.Bool("primary", false, "Start this server as the initial primary")

	flag.Parse()

	// string commas to get the list of replicas list
	replicaAddrs := []string{}
	if *replicas != "" {
		parts := strings.Split(*replicas, ",")
		for _, addr := range parts {
			trimmed := strings.TrimSpace(addr)
			if trimmed != "" {
				replicaAddrs = append(replicaAddrs, trimmed)
			}
		}
	}

	log.Printf("Starting replica server %s on %s", *id, *address)
	log.Printf("Will connect to replicas: %v", replicaAddrs)

	server, err := afs.NewReplicaServer(*id, *inputDir, *outputDir, replicaAddrs)
	if err != nil {
		log.Fatalf("Failed to create replica server: %v", err)
	}

	if *isPrimaryFlag {
		log.Printf("Server %s starting as PRIMARY", *id)
		server.BecomePrimary()
	} else {
		log.Printf("Server %s starting as BACKUP", *id)
	}

	log.Fatal(server.Start(*address))
}
