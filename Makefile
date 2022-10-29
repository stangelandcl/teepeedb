all:
	rm -rf test.db
	go test
	cd internal/file && go test
	cd internal/merge && go test
