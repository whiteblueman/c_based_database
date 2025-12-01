#include "compiler.h"
#include "input_buffer.h"
#include "table.h"
#include "vm.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// Forward declaration
void run_server(const char *filename);

int main(int argc, char *argv[]) {
  if (argc < 2) {
    printf("Must supply a database filename.\n");
    printf("Usage: %s <filename> [--server]\n", argv[0]);
    fflush(stdout);
    exit(EXIT_FAILURE);
  }

  char *filename = argv[1];

  if (argc > 2 && strcmp(argv[2], "--server") == 0) {
    run_server(filename);
    return 0;
  }

  Table *table = db_open(filename);

  InputBuffer *input_buffer = new_input_buffer();
  while (1) {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.' ||
        strcmp(input_buffer->buffer, "exit") == 0) {
      switch (do_meta_command(input_buffer, table, STDOUT_FILENO)) {
      case META_COMMAND_SUCCESS:
        continue;
      case META_COMMAND_UNRECOGNIZED_COMMAND:
        printf("Unrecognized command '%s'\n", input_buffer->buffer);
        continue;
      }
    }

    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
    case PREPARE_SUCCESS:
      break;
    case PREPARE_NEGATIVE_ID:
      printf("ID must be positive.\n");
      continue;
    case PREPARE_STRING_TOO_LONG:
      printf("String is too long.\n");
      continue;
    case PREPARE_SYNTAX_ERROR:
      printf("Syntax error. Could not parse statement.\n");
      continue;
    case PREPARE_UNRECOGNIZED_STATEMENT:
      printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
      continue;
    }

    switch (execute_statement(&statement, table, STDOUT_FILENO)) {
    case EXECUTE_SUCCESS:
      printf("Executed.\n");
      break;
    case EXECUTE_DUPLICATE_KEY:
      printf("Error: Duplicate key.\n");
      break;
    case EXECUTE_TABLE_FULL:
      printf("Error: Table full.\n");
      break;
    }
  }
}
