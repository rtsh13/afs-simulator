import afsclient

exit = False
print("AFS Client Shell")
print("Commands: open <file>, read <file> <offset> <size>, write <file> <data> <offset>, close <file>, create <file>, exit")
while not exit:
    cmd = input("> ")
    cmd_arr = cmd.split(" ")
    if not len(cmd_arr) < 0 :
        match cmd_arr[0]:
            case "open":
                print("Opening file")
                if len(cmd_arr) != 2:
                    print("err: wrong number of arguments, type help for more info")
                filename = cmd_arr[1]
                opened = afsclient.open(filename)
                if opened :
                    print("file successfully opened")
                else :
                    print("err: failed to open file ", cmd_arr[1])
            case "read":
                print("Reading file")
                if len(cmd_arr) != 4:
                    print("err: wrong number of arguments, type help for more info")
                filename = cmd_arr[1]
                offset = int(cmd_arr[2])
                size = int(cmd_arr[3])
                buffer = afsclient.read(filename, offset, size)
                if len(buffer) == 0 :
                    print("err: file read failed")
                out = buffer.decode("utf-8")
                print(out)
            case "write":
                print("Writing file")
                if len(cmd_arr) != 4:
                    print("err: wrong number of arguments, type help for more info")
                filename = cmd_arr[1]
                offset = int(cmd_arr[2])
                data = bytes(cmd_arr[3])
                size = len(data)
                written = afsclient.write(filename, data, offset, size)
                if not written:
                    print("err: failed to write to file, ", filename)
            case "close":
                print("Closeing file")
                if len(cmd_arr) != 2:
                    print("err: wrong number of arguments, type help for more info")
                filename = cmd_arr[1]
                closed = afsclient.close(filename.encode('utf-8'))
                if not closed:
                    print("err: could not close file, ", filename)
            case "create":
                print("Createing file")
                if len(cmd_arr) != 2:
                    print("err: wrong number of arguments, type help for more info")
                filename = cmd_arr[1]
                created = afsclient.create(filename)
                if not created :
                    print("err: could not create file, ", filename)
            case "exit":
                print("exiting AFS Client Shell")
                exit = True
            case "help":
                print("Commands: open <file>, read <file>, write <file> <data>, close <file>, create <file>, exit")
            case _:
                print("unknown command please try again")