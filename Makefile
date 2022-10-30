all:
	rm -rf test.db
	cd internal/file && go test
	cd internal/merge && go test
	go test
