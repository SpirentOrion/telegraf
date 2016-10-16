PROTO_SUBS =
PROTOS	= $(patsubst %.proto,%,$(wildcard *.proto))
TARGETS	= $(foreach proto,$(PROTOS),$(proto)/$(proto).pb.go)
PROTO_INC = .
IMPORT_DIR := github.com/influxdata/telegraf/proto

# Create the M flags (used by the protobuf protoc command to add the
# appropriate import path to the start of imported proto generated go
# files. If a higher-level make file provides M flags they will be added
# to the M flags generated here.
MFLAGS := $(PROTO_MFLAGS) $(foreach p,\
	$(PROTOS),M$(p).proto=$(IMPORT_DIR)/$(p))
MFLAGS := $(shell echo $(MFLAGS) | tr " " ,)

.PHONY: all clean $(PROTO_SUBS)

all: $(PROTO_SUBS) $(TARGETS)

# Auto generate the rules for building the proto files 
PROTOC_TEMPLATE = $(1)/$(1).pb.go: $(1).proto; mkdir -p $$(@D) \
	&& protoc -I $(PROTO_INC) --go_out=$(2):$$(@D) $$<
$(foreach proto,$(PROTOS),$(eval $(call \
	PROTOC_TEMPLATE,$(proto),$(MFLAGS))))

$(PROTO_SUBS):
	cd ./$@ && $(MAKE) PROTO_MFLAGS=$(MFLAGS)

clean:
	$(foreach s,$(PROTO_SUBS),cd ./$(s) && $(MAKE) $@;)
	rm -f $(TARGETS)
