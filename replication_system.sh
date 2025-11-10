
pkill -f "cmd/server"; pkill -f "coordinator.py"; pkill -f "worker.py"
pkill -f "server-1"; pkill -f "server-2"; pkill -f "server-3"
go run cmd/server/main.go -id server-1 -addr :8080 -replicas "localhost:8081,localhost:8082" -primary -input data/input -output data/output > logs/server-1.log 2>&1 &

go run cmd/server/main.go -id server-2 -addr :8081 -replicas "localhost:8080,localhost:8082" -input data/input -output data/output > logs/server-2.log 2>&1 &

go run cmd/server/main.go -id server-3 -addr :8082 -replicas "localhost:8080,localhost:8081" -input data/input -output data/output > logs/server-3.log 2>&1 &

sleep 3

python3 -u pkg/afs/coordinator.py > logs/coordinator.log 2>&1 &
sleep 2
python3 -u pkg/afs/worker.py worker-1 localhost:8080,localhost:8081,localhost:8082 5 > logs/worker1.log 2>&1 &

sleep 5
pkill -f "server-1"
tail -f logs/worker1.log