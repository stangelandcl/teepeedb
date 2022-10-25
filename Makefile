all:
	rm -rf test.db
	go test
	cd internal/merge && go test
	cd internal/test && go test
