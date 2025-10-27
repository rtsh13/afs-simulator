package main

import (
	"flag"
	"log"

	afs "github.com/afs-simulator/pkg/afs"
)

func main() {
	inputDir := flag.String("input", "./data/input", "Input directory path")
	outputDir := flag.String("output", "./data/output", "Output directory path")
	address := flag.String("addr", ":8080", "Server address")
	flag.Parse()

	server, err := afs.NewFileServer(*inputDir, *outputDir)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	log.Fatal(server.Start(*address))
}
