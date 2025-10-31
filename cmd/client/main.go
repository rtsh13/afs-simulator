package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/afs-simulator/pkg/afs"
)

func main() {
	clientID := flag.String("id", "client1", "Client ID")
	cacheDir := flag.String("cache", "/tmp/afs", "Cache directory")
	serverAddr := flag.String("server", "localhost:8080", "Server address")
	flag.Parse()

	client, err := afs.NewAFSClient(*clientID, *cacheDir, *serverAddr)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Interactive shell
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("AFS Client Shell")
	fmt.Println("Commands: open <file>, read <file>, write <file> <data>, close <file>, create <file>, exit")

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "open":
			if len(parts) < 2 {
				fmt.Println("Usage: open <filename>")
				continue
			}
			_, err := client.Open(parts[1], "rw")
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "close":
			if len(parts) < 2 {
				fmt.Println("Usage: close <filename>")
				continue
			}
			err := client.Close(parts[1])
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "create":
			if len(parts) < 2 {
				fmt.Println("Usage: create <filename>")
				continue
			}
			err := client.Create(parts[1])
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "exit":
			return

		default:
			fmt.Println("Unknown command")
		}
	}
}
