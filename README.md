# memcache_loader_go
The program parses and uploads to the memcache a minute-by-minute unloading of the installed applications tracker logs. 
The key is the type and device identifier separated by a colon, the value is a protobuf message.

# preparing
Need to add go package path to .proto file before creation of .go file: option go_package = "./" and then create .proto file for Go:
````bash
protoc  --go_out=./proto ./proto/appsinstalled.proto
````

# installation
````bash
go mod init github.com/agnusarev/memcache_loader_go
go get github.com/bradfitz/gomemcache/memcache@v0.0.0-20230611145640-acc696258285
go get google.golang.org/protobuf/cmd/protoc-gen-go@latest
````
