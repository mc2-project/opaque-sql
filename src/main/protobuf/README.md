In order to generate the gRPC files, follow the below commands to get the java gRPC plugin and auto-generate files.
Run the commands from the opaque home directory.

1. wget https://search.maven.org/remotecontent?filepath=io/grpc/protoc-gen-grpc-java/1.34.1/protoc-gen-grpc-java-1.34.1-linux-x86_64.exe
2. mv remotecontent\?filepath\=io%2Fgrpc%2Fprotoc-gen-grpc-java%2F1.34.1%2Fprotoc-gen-grpc-java-1.34.1-linux-x86_64.exe protoc-gen-grpc-java.exe
3. chmod +x protoc-gen-grpc-java.exe
4. protoc --java_out=./src/main/java src/main/protobuf/ra.proto
5. protoc --plugin=protoc-gen-grpc-java=./protoc-gen-grpc-java.exe --grpc-java_out=./src/main/java src/main/protobuf/ra.proto

The above commands in order do the following:
1. Obtains the java-grpc plugin
2. Renames the plugin into something more manageable
3. Makes the plugin executable
4. Generates the protocol buffer classes
5. Generates the gRPC files
