
.PHONY: all plugin aggregator ys jito

all: plugin aggregator ys jito

plugin:
	cargo build -p geyser-plugin-ultra --release

aggregator:
	cargo build -p ultra-aggregator --release

ys:
	cargo build -p ys-consumer --release

jito:
	./scripts/setup_protos.sh
	cargo build -p jito-client --release