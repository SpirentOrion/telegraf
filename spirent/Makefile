
# Standard Telegraf build
default: build

# Only run the build (no dependency grabbing)
build:
	echo "build target must be excplictly set.  options: migrate, prepare"

# Generate orion-res client APIs with correct path
prepare:
	echo "generating orion-res client apis..."
	cd ../vendor/github.com/SpirentOrion/orion-api/res && \
	sed 's/basePath:/#basePath:/g' ./lib/schema/v1/orion-res.yaml > ./lib/schema/v1/orion-res.processed.yaml && \
	../lib/swagger-codegen/swagger-codegen generate -i ./lib/schema/v1/orion-res.processed.yaml -l go --additional-properties packageName=v1,apiDestPath=client,modelDestPath=xfer,xferPackagePath=github.com/SpirentOrion/orion-api/res/xfer -o . && \
	goimports -w ./xfer/v1 ./client/v1

# Setup govendor dependencies 
migrate:
	./govendor.sh
