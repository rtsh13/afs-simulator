package main

import (
	"C"
	"os"
	"unsafe"

	afs "github.com/afs-simulator/pkg/afs"
)

var clientId string = os.Getenv("CLIENT_ID")
var cacheDir string = os.Getenv("CACHE_DIR")
var serverAddress string = os.Getenv("SERVER_ADDRESS")
var afsClient *afs.AFSClient = afs.NewAFSClient(clientId, cacheDir, serverAddress)

//export Open
func Open(filename *C.char) C.uint {
	if afsClient == nil {
		panic(1)
	}
	goFilename := C.GoString(filename)
	_, err := afsClient.Open(goFilename)
	if err != nil {
		return C.uint(0)
	}
	return C.uint(1)
}

//export Read
func Read(filename *C.char, offset C.int, size C.int) unsafe.Pointer {
	if afsClient == nil {
		panic(1)
	}
	goFilename := C.GoString(filename)
	goOffset := int64(offset)
	goSize := int(size)
	buffer, err := afsClient.Read(goFilename, goOffset, goSize)
	if err != nil {
		error_buffer := make([]byte, 0)
		return C.CBytes(error_buffer)
	}
	return C.CBytes(buffer)
}

//export Write
func Write(filename *C.char, data unsafe.Pointer, offset C.int, size C.int) C.uint {
	if afsClient == nil {
		panic(1)
	}
	goFilename := C.GoString(filename)
	goData := C.GoBytes(data, size)
	goOffset := int64(offset)
	if err := afsClient.Write(goFilename, goData, goOffset); err != nil {
		return C.uint(0)
	}
	return C.uint(1)
}

//export Close
func Close(filename *C.char) C.uint {
	if afsClient == nil {
		panic(1)
	}
	goFilename := C.GoString(filename)
	if err := afsClient.Close(goFilename); err != nil {
		return C.uint(0)
	}
	return C.uint(1)
}

//export Create
func Create(filename *C.char) C.uint {
	if afsClient == nil {
		panic(1)
	}
	goFilename := C.GoString(filename)
	if err := afsClient.Create(goFilename); err != nil {
		return C.uint(0)
	}
	return C.uint(1)
}

func main() {}
