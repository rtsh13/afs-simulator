#!/bin/bash

echo "Stopping Distributed Prime Finding System..."

# Kill all related processes
pkill -f "cmd/server/main.go" && echo "✓ Stopped file servers"
pkill -f "pkg/afs/coordinator.py" && echo "✓ Stopped coordinator"
pkill -f "pkg/afs/worker.py" && echo "✓ Stopped workers"

sleep 1

# Check if anything is still running
if pgrep -f "cmd/server/main.go" > /dev/null || \
   pgrep -f "pkg/afs/coordinator.py" > /dev/null || \
   pgrep -f "pkg/afs/worker.py" > /dev/null; then
    echo "Warning: Some processes may still be running"
    echo "Use 'ps aux | grep -E \"(server|coordinator|worker)\"' to check"
else
    echo "All processes stopped successfully"
fi

echo
echo "System shutdown complete"