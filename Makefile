all: build
build:
	mkdir -p ./packages
	pipenv lock -r > requirements.txt
	pip3 install -r requirements.txt --target ./packages
	touch ./packages/empty.txt
	cd packages && zip -r packages.zip  .
	zip -ur ./packages/packages.zip utils -x utils/__pycache__/\*
	cp ./packages/packages.zip .
clean:
	rm -r ./packages
