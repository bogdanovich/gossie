gen-go: cassandra.thrift
	rm -rf ./src/cassandra
	thrift --gen go:package_prefix=github.com/betable/gossie -out ./src ./cassandra.thrift
	sed -E -i.orig 's#git.apache.org/thrift.git/lib/go/thrift#github.com/betable/go-thrift/thrift#' src/cassandra/*.go
	rm src/cassandra/*.orig
