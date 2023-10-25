test:
	docker-compose -f ./docker-compose.test.yml up -d
	sleep 2
	go test --tags=integration ./...
	docker-compose -f ./docker-compose.test.yml down & true

.SILENT: test 
.PHONY: test
