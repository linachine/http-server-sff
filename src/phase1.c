#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>

#define HOST "localhost"      // Where server listens
#define PORT 8080             // Door number
#define CHUNK_SIZE 4096       // Send files in 4KB pieces
#define BUFFER_SIZE 1024      // Read requests in 1KB chunks

// Find which file they want from "GET /filename HTTP/1.1"
char* parse_request(char* data) {
    if (strncmp(data, "GET /", 5) == 0) {
        char* path_start = data + 4;   // Skip "GET "
        char* space = strchr(path_start, ' ');  // Find end of path
        if (space) {
            *space = '\0';            // Null terminate path
            return path_start + 1;    // Skip leading "/"
        }
    }
    return NULL;
}

// What type of file is it? (for web browsers)
char* get_content_type(char* filename) {
    if (strstr(filename, ".html")) return "text/html";
    if (strstr(filename, ".txt"))  return "text/plain";
    return "application/octet-stream";
}

// send() is NOT guaranteed to send all bytes, so we loop until done
ssize_t send_all(int fd, const void* buf, size_t len) {
    size_t total = 0;
    const char* p = buf;

    while (total < len) {
        ssize_t sent = send(fd, p + total, len - total, 0);
        if (sent <= 0) return -1;
        total += sent;
    }
    return total;
}

int main() {
    int server_fd, client_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len = sizeof(client_addr);
    char buffer[BUFFER_SIZE];
    char request[BUFFER_SIZE * 4];  // Bigger buffer for full request
    int request_len = 0;

    // Step 1: Create server socket (like opening a door)
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("Socket creation failed");
        return 1;
    }

    // Allow reuse of port (no waiting after restart)
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Step 2: Setup server address
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    server_addr.sin_port = htons(PORT);

    // Bind and listen (open the door for 1 customer at a time)
    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind failed");
        return 1;
    }
    listen(server_fd, 1);
    printf(" Server ready at http://localhost:%d\n", PORT);

    // Step 3: Wait for customers forever
    while (1) {
        client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            perror("Accept failed");
            continue;
        }

        printf(" New visitor!\n");

        // Step 4: Read their web request until headers end
        request_len = 0;
        memset(request, 0, sizeof(request));

        while (request_len < (int)sizeof(request) - 1) {
            int bytes = recv(client_fd, buffer, BUFFER_SIZE - 1, 0);
            if (bytes <= 0) break;

            buffer[bytes] = '\0';
            strncat(request, buffer, sizeof(request) - strlen(request) - 1);
            request_len += bytes;

            // Stop when we see end of headers "\r\n\r\n"
            if (strstr(request, "\r\n\r\n")) break;
        }

        // Step 5: Find what file they want
        char* filename = parse_request(request);
        if (!filename || strlen(filename) == 0) {
            filename = "index.html";  // Default home page
        }

        // Step 6: Safety - block "../" tricks
        if (strstr(filename, "..") || strchr(filename, '/')) {
            char* bad =
                "HTTP/1.0 400 Bad Request\r\n"
                "Content-Type: text/plain\r\n"
                "Content-Length: 11\r\n"
                "\r\n"
                "Bad request";
            send_all(client_fd, bad, strlen(bad));
            close(client_fd);
            continue;
        }

        // Step 7: Check if file exists and send it
        FILE* file = fopen(filename, "rb");
        if (file) {
            // Get file size
            fseek(file, 0, SEEK_END);
            long file_size = ftell(file);
            fseek(file, 0, SEEK_SET);

            char* content_type = get_content_type(filename);

            // Send headers first (like package label)
            char headers[512];
            sprintf(headers,
                "HTTP/1.0 200 OK\r\n"
                "Content-Type: %s\r\n"
                "Content-Length: %ld\r\n"
                "\r\n",
                content_type, file_size);

            send_all(client_fd, headers, strlen(headers));

            // Send file in small pieces (good for big files)
            char chunk[CHUNK_SIZE];
            while (1) {
                size_t bytes_read = fread(chunk, 1, CHUNK_SIZE, file);
                if (bytes_read == 0) break;
                send_all(client_fd, chunk, bytes_read);
            }

            fclose(file);
            printf(" Sent %s\n", filename);
        } else {
            // File not found
            const char* body = "File not found";
            char not_found[256];
            sprintf(not_found,
                "HTTP/1.0 404 Not Found\r\n"
                "Content-Type: text/plain\r\n"
                "Content-Length: %zu\r\n"
                "\r\n"
                "%s",
                strlen(body), body);

            send_all(client_fd, not_found, strlen(not_found));
            printf(" %s not found\n", filename);
        }

        close(client_fd);  // Close connection
    }

    close(server_fd);
    return 0;
}
