CC = clang
CFLAGS = -g -Wall -Wextra -I./include

SRC_DIR = src
OBJ_DIR = obj
BIN_DIR = .

SRCS = $(wildcard $(SRC_DIR)/*.c)
OBJS = obj/compiler.o obj/cursor.o obj/input_buffer.o obj/main.o obj/node.o obj/pager.o obj/table.o obj/vm.o obj/server.o
TARGET = $(BIN_DIR)/db

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c -o $@ $<

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

clean:
	rm -rf $(OBJ_DIR) $(TARGET)

.PHONY: all clean
