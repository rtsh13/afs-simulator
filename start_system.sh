#!/bin/bash
echo "Starting Distributed Prime Finding System..."

# Create directories
mkdir -p data/input data/output logs snapshots /tmp/afs

# Kill old processes
pkill -f "cmd/server/main.go" 2>/dev/null || true
pkill -f "coordinator.py" 2>/dev/null || true
pkill -f "worker.py" 2>/dev/null || true
sleep 1

# Start file server
echo "Starting file server..."
go run cmd/server/main.go \
    -input ./data/input \
    -output ./data/output \
    -addr :8080 \
    > logs/server.log 2>&1 &

sleep 2

# Start coordinator
echo "Starting coordinator..."
python3 -u pkg/afs/coordinator.py > logs/coordinator.log 2>&1 &
sleep 2

# Start 3 workers
echo "Starting workers..."
python3 -u pkg/afs/worker.py worker-1 > logs/worker1.log 2>&1 &
python3 -u pkg/afs/worker.py worker-2 > logs/worker2.log 2>&1 &
python3 -u pkg/afs/worker.py worker-3 > logs/worker3.log 2>&1 &
sleep 1

echo
echo "System started!"
echo "- Server: localhost:8080"
echo "- Coordinator: localhost:5000"
echo "- Workers: 3"
echo
echo "To view logs:"
echo "  tail -f logs/server.log"
echo "  tail -f logs/coordinator.log"
echo "  tail -f logs/worker1.log"
echo
echo "To watch progress:"
echo "  watch -n 1 'wc -l primes.txt'"
echo
echo "To stop:"
echo "  ./stop_system.sh"