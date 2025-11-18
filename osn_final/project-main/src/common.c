#include "../include/common.h"
#include <stdarg.h>

// Error messages
const char* error_messages[] = {
    "Success",
    "File not found",
    "Unauthorized access",
    "File or sentence is locked",
    "Invalid sentence or word index",
    "File already exists",
    "Permission denied",
    "Invalid command",
    "Storage server unavailable",
    "Internal server error",
    "User not found",
    "No storage servers available",
    "Invalid parameters",
    "Execution failed"
};

char* get_error_message(int error_code) {
    if (error_code >= 0 && error_code <= 13) {
        return (char*)error_messages[error_code];
    }
    return "Unknown error";
}

char* get_timestamp() {
    static char buffer[64];
    time_t now = time(NULL);
    struct tm* tm_info = localtime(&now);
    strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", tm_info);
    return buffer;
}

void log_message(const char* component, const char* level, const char* format, ...) {
    va_list args;
    char log_buffer[2048];
    
    va_start(args, format);
    vsnprintf(log_buffer, sizeof(log_buffer), format, args);
    va_end(args);
    
    printf("[%s] [%s] [%s] %s\n", get_timestamp(), component, level, log_buffer);
    fflush(stdout);
    
    // Also write to log file
    char log_filename[256];
    snprintf(log_filename, sizeof(log_filename), "logs/%s.log", component);
    
    FILE* log_file = fopen(log_filename, "a");
    if (log_file) {
        fprintf(log_file, "[%s] [%s] %s\n", get_timestamp(), level, log_buffer);
        fclose(log_file);
    }
}

void send_message(int socket_fd, Message* msg) {
    int total_sent = 0;
    int bytes_to_send = sizeof(Message);
    char* buffer = (char*)msg;
    
    while (total_sent < bytes_to_send) {
        int sent = send(socket_fd, buffer + total_sent, bytes_to_send - total_sent, 0);
        if (sent <= 0) {
            log_message("COMMON", "ERROR", "Failed to send message: %s", strerror(errno));
            return;
        }
        total_sent += sent;
    }
}

int receive_message(int socket_fd, Message* msg) {
    int total_received = 0;
    int bytes_to_receive = sizeof(Message);
    char* buffer = (char*)msg;
    
    while (total_received < bytes_to_receive) {
        int received = recv(socket_fd, buffer + total_received, bytes_to_receive - total_received, 0);
        if (received <= 0) {
            if (received == 0) {
                log_message("COMMON", "INFO", "Connection closed by peer");
            } else {
                log_message("COMMON", "ERROR", "Failed to receive message: %s", strerror(errno));
            }
            return -1;
        }
        total_received += received;
    }
    
    return 0;
}
