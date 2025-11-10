#!/bin/bash

# Kill all processes and wait for ports to be released
echo "Killing any existing processes..."
pkill -9 -f "cmd/server" 2>/dev/null || true
pkill -9 -f "coordinator.py" 2>/dev/null || true
pkill -9 -f "worker.py" 2>/dev/null || true
pkill -9 -f "server-1" 2>/dev/null || true
pkill -9 -f "server-2" 2>/dev/null || true
pkill -9 -f "server-3" 2>/dev/null || true
echo "Waiting for ports to be released..."
sleep 5

mkdir -p logs/test1 logs/test2 logs/test3 snapshots data/input data/output

# Test 1: Basic Snapshot Creation
echo "========================================="
echo "TEST 1: Basic Snapshot Creation"
echo "========================================="

# Start 3 AFS replicas
go run cmd/server/main.go -id server-1 -addr :8080 -replicas "localhost:8081,localhost:8082" -primary -input data/input -output data/output > logs/test1/server-1.log 2>&1 &
sleep 1
go run cmd/server/main.go -id server-2 -addr :8081 -replicas "localhost:8080,localhost:8082" -input data/input -output data/output > logs/test1/server-2.log 2>&1 &
sleep 1
go run cmd/server/main.go -id server-3 -addr :8082 -replicas "localhost:8080,localhost:8081" -input data/input -output data/output > logs/test1/server-3.log 2>&1 &
sleep 2

# Start coordinator
python3 -u pkg/afs/coordinator.py > logs/test1/coordinator.log 2>&1 &
sleep 2

# Start 3 workers
python3 -u pkg/afs/worker.py worker-1 localhost:8080,localhost:8081,localhost:8082 5 > logs/test1/worker1.log 2>&1 &
python3 -u pkg/afs/worker.py worker-2 localhost:8080,localhost:8081,localhost:8082 5 > logs/test1/worker2.log 2>&1 &
python3 -u pkg/afs/worker.py worker-3 localhost:8080,localhost:8081,localhost:8082 5 > logs/test1/worker3.log 2>&1 &
sleep 2

# Wait for snapshots
echo "Waiting for snapshots (45s)..."
sleep 45

snapshot_count=$(ls -1 snapshots 2>/dev/null | wc -l)
if [ $snapshot_count -gt 0 ]; then
    echo "Snapshots created: $snapshot_count"
    ls -lh snapshots/
else
    echo "No snapshots created"
fi

echo "Test 1 logs: logs/test1/"
echo ""

# Cleanup
echo "Cleaning up Test 1..."
pkill -9 -f "cmd/server" 2>/dev/null || true
pkill -9 -f "coordinator.py" 2>/dev/null || true
pkill -9 -f "worker.py" 2>/dev/null || true
pkill -9 -f "server-1" 2>/dev/null || true
pkill -9 -f "server-2" 2>/dev/null || true
pkill -9 -f "server-3" 2>/dev/null || true
echo "Waiting for ports to be released..."
sleep 8

# Test 2: Worker Failure Recovery
echo "========================================="
echo "TEST 2: Worker Failure Recovery"
echo "========================================="

# Start replicas
go run cmd/server/main.go -id server-1 -addr :8080 -replicas "localhost:8081,localhost:8082" -primary -input data/input -output data/output > logs/test2/server-1.log 2>&1 &
sleep 1
go run cmd/server/main.go -id server-2 -addr :8081 -replicas "localhost:8080,localhost:8082" -input data/input -output data/output > logs/test2/server-2.log 2>&1 &
sleep 1
go run cmd/server/main.go -id server-3 -addr :8082 -replicas "localhost:8080,localhost:8081" -input data/input -output data/output > logs/test2/server-3.log 2>&1 &
sleep 2

# Start coordinator
python3 -u pkg/afs/coordinator.py > logs/test2/coordinator.log 2>&1 &
sleep 2

# Start workers
python3 -u pkg/afs/worker.py worker-1 localhost:8080,localhost:8081,localhost:8082 5 > logs/test2/worker1.log 2>&1 &
python3 -u pkg/afs/worker.py worker-2 localhost:8080,localhost:8081,localhost:8082 5 > logs/test2/worker2.log 2>&1 &
python3 -u pkg/afs/worker.py worker-3 localhost:8080,localhost:8081,localhost:8082 5 > logs/test2/worker3.log 2>&1 &
sleep 2

echo "System running for 15s (let worker-1 start processing)..."
sleep 15

echo "Killing worker-1 mid-task..."
pkill -f "worker.py worker-1"
sleep 3

echo "Restarting worker-1..."
python3 -u pkg/afs/worker.py worker-1 localhost:8080,localhost:8081,localhost:8082 5 > logs/test2/worker1-restart.log 2>&1 &
sleep 10

if grep -q "registered" logs/test2/worker1-restart.log; then
    echo "Worker-1 recovered successfully"
    echo "Checking if remaining workers took over tasks..."
    sleep 10
    
    completed_after=$(grep -c "Complete" logs/test2/worker2.log 2>/dev/null || echo 0)
    echo "Worker-2 completed tasks: $completed_after"
else
    echo "Worker-1 failed to recover"
fi

echo "Test 2 logs: logs/test2/"
echo ""

# Cleanup
echo "Cleaning up Test 2..."
pkill -9 -f "cmd/server" 2>/dev/null || true
pkill -9 -f "coordinator.py" 2>/dev/null || true
pkill -9 -f "worker.py" 2>/dev/null || true
pkill -9 -f "server-1" 2>/dev/null || true
pkill -9 -f "server-2" 2>/dev/null || true
pkill -9 -f "server-3" 2>/dev/null || true
echo "Waiting for ports to be released..."
sleep 8

# Test 3: Primary Server Failure
echo "========================================="
echo "TEST 3: Primary Server Failure - Failover"
echo "========================================="

# Start replicas
go run cmd/server/main.go -id server-1 -addr :8080 -replicas "localhost:8081,localhost:8082" -primary -input data/input -output data/output > logs/test3/server-1.log 2>&1 &
sleep 1
go run cmd/server/main.go -id server-2 -addr :8081 -replicas "localhost:8080,localhost:8082" -input data/input -output data/output > logs/test3/server-2.log 2>&1 &
sleep 1
go run cmd/server/main.go -id server-3 -addr :8082 -replicas "localhost:8080,localhost:8081" -input data/input -output data/output > logs/test3/server-3.log 2>&1 &
sleep 2

# Start coordinator
python3 -u pkg/afs/coordinator.py > logs/test3/coordinator.log 2>&1 &
sleep 2

# Start workers
python3 -u pkg/afs/worker.py worker-1 localhost:8080,localhost:8081,localhost:8082 5 > logs/test3/worker1.log 2>&1 &
python3 -u pkg/afs/worker.py worker-2 localhost:8080,localhost:8081,localhost:8082 5 > logs/test3/worker2.log 2>&1 &
sleep 2

echo "System running for 15s..."
sleep 15

echo "Killing primary server (server-1 on :8080)..."
pkill -f "server-1"
sleep 3

echo "System continuing for 30s..."
sleep 30

worker1_tasks=$(grep -c "Complete" logs/test3/worker1.log 2>/dev/null || echo 0)
worker2_tasks=$(grep -c "Complete" logs/test3/worker2.log 2>/dev/null || echo 0)

if [ $worker1_tasks -gt 0 ] || [ $worker2_tasks -gt 0 ]; then
    echo "Workers continued after primary failure"
    echo "  Worker-1 tasks: $worker1_tasks"
    echo "  Worker-2 tasks: $worker2_tasks"
else
    echo "Workers didn't continue processing"
fi

echo "Test 3 logs: logs/test3/"
echo ""

# Cleanup
echo "Cleaning up Test 3..."
pkill -9 -f "cmd/server" 2>/dev/null || true
pkill -9 -f "coordinator.py" 2>/dev/null || true
pkill -9 -f "worker.py" 2>/dev/null || true
pkill -9 -f "server-1" 2>/dev/null || true
pkill -9 -f "server-2" 2>/dev/null || true
pkill -9 -f "server-3" 2>/dev/null || true
sleep 3

echo ""
echo "========================================="
echo "All tests completed"
echo "========================================="
echo "Test 1 logs: logs/test1/"
echo "Test 2 logs: logs/test2/"
echo "Test 3 logs: logs/test3/"
echo "Snapshots: snapshots/"