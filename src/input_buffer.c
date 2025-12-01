#include "input_buffer.h"
#include <readline/history.h>
#include <readline/readline.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

InputBuffer *new_input_buffer() {
  InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;
  return input_buffer;
}

void print_prompt() {
  // Prompt is handled by readline
}

void read_input(InputBuffer *input_buffer) {
  char *line = readline("db > ");

  if (line == NULL) {
    // EOF
    exit(EXIT_SUCCESS);
  }

  if (line[0] != '\0') {
    add_history(line);
  }

  input_buffer->input_length = strlen(line);
  input_buffer->buffer = line;
  input_buffer->buffer_length = input_buffer->input_length + 1;
}

void close_input_buffer(InputBuffer *input_buffer) {
  free(input_buffer->buffer); // readline allocates this
  free(input_buffer);
}
