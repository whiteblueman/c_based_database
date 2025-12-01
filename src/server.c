#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "compiler.h"
#include "input_buffer.h"
#include "table.h"
#include "vm.h"

#define PORT 8088
#define BUFFER_SIZE 1024

void run_server(const char *filename) {
  int server_fd, new_socket;
  struct sockaddr_in address;
  int opt = 1;
  int addrlen = sizeof(address);
  char buffer[BUFFER_SIZE] = {0};

  // Creating socket file descriptor
  if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
    perror("socket failed");
    exit(EXIT_FAILURE);
  }

  // Forcefully attaching socket to the port 8080
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
    perror("setsockopt");
    exit(EXIT_FAILURE);
  }
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(PORT);

  if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
    perror("bind failed");
    exit(EXIT_FAILURE);
  }
  if (listen(server_fd, 3) < 0) {
    perror("listen");
    exit(EXIT_FAILURE);
  }

  printf("Server listening on port %d\n", PORT);

  Table *table = db_open(filename);
  InputBuffer *input_buffer = new_input_buffer();

  while (1) {
    if ((new_socket = accept(server_fd, (struct sockaddr *)&address,
                             (socklen_t *)&addrlen)) < 0) {
      perror("accept");
      continue;
    }

    printf("New connection accepted\n");

    while (1) {
      // Clear buffer
      memset(buffer, 0, BUFFER_SIZE);

      // Read from client
      int valread = read(new_socket, buffer, BUFFER_SIZE);
      printf("Debug: Read %d bytes: '%s'\n", valread, buffer);
      if (valread <= 0) {
        // Client disconnected
        close(new_socket);
        printf("Client disconnected\n");
        break;
      }

      // Remove newline at end if present
      // Remove newline at end if present
      if (valread > 0 && buffer[valread - 1] == '\n') {
        buffer[valread - 1] = '\0';
        valread--;
      }
      if (valread > 0 && buffer[valread - 1] == '\r') {
        buffer[valread - 1] = '\0';
        valread--;
      }

      // Copy to input buffer
      // Note: input_buffer->buffer is usually managed by getline, but here we
      // manually set it We need to ensure input_buffer->buffer is large enough
      // or realloc For simplicity, let's just use strncpy if it fits, or
      // realloc Our InputBuffer struct has buffer and buffer_length. Let's just
      // use the buffer directly or update InputBuffer. Actually,
      // prepare_statement uses input_buffer->buffer.

      // Let's resize input_buffer if needed
      if (input_buffer->buffer_length < (size_t)valread + 1) {
        input_buffer->buffer = realloc(input_buffer->buffer, valread + 1);
        input_buffer->buffer_length = valread + 1;
      }
      strcpy(input_buffer->buffer, buffer);
      input_buffer->input_length = strlen(buffer);

      // Handle Meta Commands
      if (input_buffer->buffer[0] == '.') {
        switch (do_meta_command(input_buffer, table, new_socket)) {
        case META_COMMAND_SUCCESS:
          // .exit means disconnect client, not shutdown server
          if (strcmp(input_buffer->buffer, ".exit") == 0) {
            close(new_socket);
            goto client_disconnected;
          }
          continue;
        case META_COMMAND_UNRECOGNIZED_COMMAND:
          dprintf(new_socket, "Unrecognized command '%s'\n",
                  input_buffer->buffer);
          continue;
        }
      }

      Statement statement;
      switch (prepare_statement(input_buffer, &statement)) {
      case PREPARE_SUCCESS:
        break;
      case PREPARE_NEGATIVE_ID:
        dprintf(new_socket, "ID must be positive.\n");
        continue;
      case PREPARE_STRING_TOO_LONG:
        dprintf(new_socket, "String is too long.\n");
        continue;
      case PREPARE_SYNTAX_ERROR:
        dprintf(new_socket, "Syntax error. Could not parse statement.\n");
        continue;
      case PREPARE_UNRECOGNIZED_STATEMENT:
        dprintf(new_socket, "Unrecognized keyword at start of '%s'.\n",
                input_buffer->buffer);
        continue;
      }
      printf("Debug: Calling execute_statement\n");
      fflush(stdout);

      switch (execute_statement(&statement, table, new_socket)) {
      case EXECUTE_SUCCESS:
        dprintf(new_socket, "Executed.\n");
        break;
      case EXECUTE_DUPLICATE_KEY:
        dprintf(new_socket, "Error: Duplicate key.\n");
        break;
      case EXECUTE_TABLE_FULL:
        dprintf(new_socket, "Error: Table full.\n");
        break;
      }
    }
  client_disconnected:;
  }
}
