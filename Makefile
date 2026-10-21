ERLC ?= erlc
ERL ?= erl
BUILD_DIR := ebin
MODULE := $(BUILD_DIR)/simple_kv.beam

.PHONY: all compile shell clean

all: compile

compile: $(MODULE)

$(MODULE): main.erl
	mkdir -p $(BUILD_DIR)
	cp $< $(BUILD_DIR)/simple_kv.erl
	$(ERLC) -Werror -o $(BUILD_DIR) $(BUILD_DIR)/simple_kv.erl

shell:
	$(ERL) -pa $(BUILD_DIR)

clean:
	rm -rf $(BUILD_DIR)
