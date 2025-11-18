#include "../include/common.h"
#include <signal.h>
#include <ctype.h>
#include <unistd.h>

int nm_socket = -1;
char username[MAX_USERNAME];
int running = 1;

void cleanup_client() {
    running = 0;
    if (nm_socket >= 0) {
        close(nm_socket);
    }
}

void signal_handler_client(int signum) {
    (void)signum;  // Mark as intentionally unused
    printf("\nExiting...\n");
    cleanup_client();
    exit(0);
}

int connect_to_nm() {
    int retry_count = 0;
    int max_retries = 3;
    
    while (retry_count < max_retries) {
        nm_socket = socket(AF_INET, SOCK_STREAM, 0);
        if (nm_socket < 0) {
            printf("ERROR: Failed to create socket\n");
            return -1;
        }
        
        // Set socket timeout
        struct timeval timeout;
        timeout.tv_sec = 5;
        timeout.tv_usec = 0;
        setsockopt(nm_socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        setsockopt(nm_socket, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    
    struct sockaddr_in nm_addr;
    memset(&nm_addr, 0, sizeof(nm_addr));
    nm_addr.sin_family = AF_INET;
    nm_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    nm_addr.sin_port = htons(NM_PORT);
    
    if (connect(nm_socket, (struct sockaddr*)&nm_addr, sizeof(nm_addr)) < 0) {
        if (retry_count < max_retries - 1) {
            printf("Connection failed, retrying (%d/%d)...\n", retry_count + 1, max_retries);
            close(nm_socket);
            retry_count++;
            sleep(2);
            continue;
        }
        printf("ERROR: Failed to connect to Name Server after %d attempts: %s\n", 
               max_retries, strerror(errno));
        close(nm_socket);
        return -1;
    }
    
    printf("Connected to Name Server\n\n");
    
    // Register user
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_REGISTER_USER;
    strncpy(msg.username, username, MAX_USERNAME - 1);
    snprintf(msg.data, BUFFER_SIZE, "127.0.0.1|0");
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) >= 0 && response.error_code == SUCCESS) {
        printf("Registered as: %s\n\n", username);
        return 0;
    } else {
        printf("ERROR: Failed to register with Name Server\n");
        close(nm_socket);
        return -1;
    }
    }
    
    return -1;  // Should never reach here
}

void cmd_view(char* args) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_VIEW;
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.data, args, BUFFER_SIZE - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0) {
        printf("ERROR: Communication failed\n");
        return;
    }
    
    if (response.error_code != SUCCESS) {
        printf("ERROR: %s\n", get_error_message(response.error_code));
        return;
    }
    
    if (strlen(response.data) == 0) {
        printf("No files found.\n");
        return;
    }
    
    printf("Files:\n");
    char* token = strtok(response.data, "|");
    while (token != NULL) {
        if (strlen(token) > 0) {
            printf("--> %s\n", token);
            
            // Skip owner, word_count, char_count if present
            token = strtok(NULL, "|");  // owner
            token = strtok(NULL, "|");  // word_count
            token = strtok(NULL, "|");  // char_count
        }
        token = strtok(NULL, "|");
    }
    printf("\n");
}

void cmd_create(char* filename) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_CREATE;
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0) {
        printf("ERROR: Communication failed\n");
        return;
    }
    
    if (response.error_code != SUCCESS) {
        printf("ERROR: %s\n", get_error_message(response.error_code));
        return;
    }
    
    // Get SS info
    char ss_ip[64];
    int ss_port;
    sscanf(response.data, "%[^|]|%d", ss_ip, &ss_port);
    
    // Connect to SS
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (ss_socket < 0) {
        printf("ERROR: Failed to connect to storage server\n");
        return;
    }
    
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss_ip);
    ss_addr.sin_port = htons(ss_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        printf("ERROR: Failed to connect to storage server\n");
        close(ss_socket);
        return;
    }
    
    // Send CREATE to SS
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.msg_type = MSG_SS_COMMAND;
    ss_msg.command = CMD_CREATE;
    strncpy(ss_msg.username, username, MAX_USERNAME - 1);
    strncpy(ss_msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) >= 0) {
        if (ss_response.error_code == SUCCESS) {
            printf("File '%s' created successfully!\n\n", filename);
        } else {
            printf("ERROR: %s\n\n", get_error_message(ss_response.error_code));
        }
    }
    
    close(ss_socket);
}

void cmd_read(char* filename) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_READ;
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0) {
        printf("ERROR: Communication failed\n");
        return;
    }
    
    if (response.error_code != SUCCESS) {
        printf("ERROR: %s\n\n", get_error_message(response.error_code));
        return;
    }
    
    // Get SS info
    char ss_ip[64];
    int ss_port;
    sscanf(response.data, "%[^|]|%d", ss_ip, &ss_port);
    
    // Connect to SS
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss_ip);
    ss_addr.sin_port = htons(ss_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        printf("ERROR: Failed to connect to storage server\n");
        close(ss_socket);
        return;
    }
    
    // Send READ to SS
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.command = CMD_READ;
    strncpy(ss_msg.username, username, MAX_USERNAME - 1);
    strncpy(ss_msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) >= 0) {
        if (ss_response.error_code == SUCCESS) {
            printf("%s\n\n", ss_response.data);
        } else {
            printf("ERROR: %s\n\n", get_error_message(ss_response.error_code));
        }
    }
    
    close(ss_socket);
}

void cmd_write(char* filename, int sentence_index) {
    // Step 1: Acquire lock from Name Server
    printf("Acquiring lock for %s sentence %d...\n", filename, sentence_index);
    
    Message lock_msg;
    memset(&lock_msg, 0, sizeof(Message));
    lock_msg.msg_type = MSG_COMMAND;
    lock_msg.command = CMD_LOCK_ACQUIRE;
    strncpy(lock_msg.username, username, MAX_USERNAME - 1);
    strncpy(lock_msg.filename, filename, MAX_FILENAME - 1);
    snprintf(lock_msg.data, BUFFER_SIZE, "%d", sentence_index);
    
    send_message(nm_socket, &lock_msg);
    
    Message lock_response;
    if (receive_message(nm_socket, &lock_response) < 0) {
        printf("ERROR: Failed to acquire lock\n\n");
        return;
    }
    
    if (lock_response.error_code != SUCCESS) {
        printf("ERROR: %s\n\n", get_error_message(lock_response.error_code));
        return;
    }
    
    printf("Lock acquired!\n");
    
    // Get SS info from lock response
    char ss_ip[64];
    int ss_port;
    sscanf(lock_response.data, "%[^|]|%d", ss_ip, &ss_port);
    
    // Step 2: Enter edit mode
    printf("Write mode: %s sentence %d\n", filename, sentence_index);
    printf("Enter edits as: <word_index> <content>\n");
    printf("Type ETIRW when done\n\n");
    
    char write_data[BUFFER_SIZE];
    int pos = 0;
    pos += snprintf(write_data, BUFFER_SIZE, "%d|", sentence_index);
    
    char line[512];
    while (fgets(line, sizeof(line), stdin)) {
        // Remove newline
        line[strcspn(line, "\n")] = 0;
        
        if (strcmp(line, "ETIRW") == 0) {
            break;
        }
        
        // Parse word_index and word
        int word_index;
        char word[MAX_WORD_LENGTH];
        if (sscanf(line, "%d %s", &word_index, word) == 2) {
            pos += snprintf(write_data + pos, BUFFER_SIZE - pos, "%d|%s|", word_index, word);
            printf("Added edit: word %d = \"%s\"\n", word_index, word);
        } else {
            printf("Invalid format. Use: <word_index> <content>\n");
        }
    }
    
    printf("Committing write...\n");
    
    // Step 3: Connect to SS and commit write
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss_ip);
    ss_addr.sin_port = htons(ss_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        printf("ERROR: Failed to connect to storage server\n\n");
        close(ss_socket);
        
        // Release lock on failure
        Message release_msg;
        memset(&release_msg, 0, sizeof(Message));
        release_msg.msg_type = MSG_COMMAND;
        release_msg.command = CMD_LOCK_RELEASE;
        strncpy(release_msg.username, username, MAX_USERNAME - 1);
        strncpy(release_msg.filename, filename, MAX_FILENAME - 1);
        snprintf(release_msg.data, BUFFER_SIZE, "%d", sentence_index);
        send_message(nm_socket, &release_msg);
        receive_message(nm_socket, &lock_response);
        
        return;
    }
    
    // Send WRITE_COMMIT to SS
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.command = CMD_WRITE_COMMIT;
    strncpy(ss_msg.username, username, MAX_USERNAME - 1);
    strncpy(ss_msg.filename, filename, MAX_FILENAME - 1);
    strncpy(ss_msg.data, write_data, BUFFER_SIZE - 1);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) >= 0) {
        if (ss_response.error_code == SUCCESS) {
            printf("Write successful!\n");
        } else {
            printf("ERROR: %s\n", get_error_message(ss_response.error_code));
        }
    }
    
    close(ss_socket);
    
    // Step 4: Release lock
    printf("Releasing lock...\n");
    Message release_msg;
    memset(&release_msg, 0, sizeof(Message));
    release_msg.msg_type = MSG_COMMAND;
    release_msg.command = CMD_LOCK_RELEASE;
    strncpy(release_msg.username, username, MAX_USERNAME - 1);
    strncpy(release_msg.filename, filename, MAX_FILENAME - 1);
    snprintf(release_msg.data, BUFFER_SIZE, "%d", sentence_index);
    
    send_message(nm_socket, &release_msg);
    
    if (receive_message(nm_socket, &lock_response) >= 0) {
        if (lock_response.error_code == SUCCESS) {
            printf("Lock released!\n\n");
        }
    }
}

void cmd_delete(char* filename) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_DELETE;
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0) {
        printf("ERROR: Communication failed\n");
        return;
    }
    
    if (response.error_code == SUCCESS) {
        printf("File '%s' deleted successfully!\n\n", filename);
    } else {
        printf("ERROR: %s\n\n", get_error_message(response.error_code));
    }
}

void cmd_list() {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_LIST;
    strncpy(msg.username, username, MAX_USERNAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0) {
        printf("ERROR: Communication failed\n");
        return;
    }
    
    if (response.error_code != SUCCESS) {
        printf("ERROR: %s\n", get_error_message(response.error_code));
        return;
    }
    
    printf("Users:\n");
    char* token = strtok(response.data, "|");
    while (token != NULL) {
        if (strlen(token) > 0) {
            printf("--> %s\n", token);
        }
        token = strtok(NULL, "|");
    }
    printf("\n");
}

void cmd_info(char* filename) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_READ;  // Get SS info first
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0 || response.error_code != SUCCESS) {
        printf("ERROR: Failed to get storage server info\n\n");
        return;
    }
    
    char ss_ip[64];
    int ss_port;
    sscanf(response.data, "%[^|]|%d", ss_ip, &ss_port);
    
    // Connect to SS
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss_ip);
    ss_addr.sin_port = htons(ss_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        printf("ERROR: Failed to connect to storage server\n\n");
        close(ss_socket);
        return;
    }
    
    // Send INFO to SS
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.command = CMD_INFO;
    strncpy(ss_msg.username, username, MAX_USERNAME - 1);
    strncpy(ss_msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) >= 0) {
        if (ss_response.error_code == SUCCESS) {
            printf("\n%s\n\n", ss_response.data);
        } else {
            printf("ERROR: %s\n\n", get_error_message(ss_response.error_code));
        }
    }
    
    close(ss_socket);
}

void cmd_fileinfo(char* filename) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_READ;  // Get SS info first
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0 || response.error_code != SUCCESS) {
        printf("ERROR: Failed to get storage server info\n\n");
        return;
    }
    
    char ss_ip[64];
    int ss_port;
    sscanf(response.data, "%[^|]|%d", ss_ip, &ss_port);
    
    // Connect to SS
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss_ip);
    ss_addr.sin_port = htons(ss_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        printf("ERROR: Failed to connect to storage server\n\n");
        close(ss_socket);
        return;
    }
    
    // Send FILEINFO to SS
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.command = CMD_FILEINFO;
    strncpy(ss_msg.username, username, MAX_USERNAME - 1);
    strncpy(ss_msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) >= 0) {
        if (ss_response.error_code == SUCCESS) {
            printf("\n%s\n", ss_response.data);
        } else {
            printf("ERROR: %s\n\n", get_error_message(ss_response.error_code));
        }
    }
    
    close(ss_socket);
}

void cmd_copy(char* source, char* destination) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_READ;  // Get SS info for source file
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, source, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0 || response.error_code != SUCCESS) {
        printf("ERROR: Failed to get storage server info\n\n");
        return;
    }
    
    char ss_ip[64];
    int ss_port;
    sscanf(response.data, "%[^|]|%d", ss_ip, &ss_port);
    
    // Connect to SS
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss_ip);
    ss_addr.sin_port = htons(ss_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        printf("ERROR: Failed to connect to storage server\n\n");
        close(ss_socket);
        return;
    }
    
    // Send COPY to SS
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.command = CMD_COPY;
    strncpy(ss_msg.username, username, MAX_USERNAME - 1);
    snprintf(ss_msg.data, BUFFER_SIZE, "%s|%s", source, destination);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) >= 0) {
        if (ss_response.error_code == SUCCESS) {
            printf("SUCCESS: %s\n\n", ss_response.data);
        } else {
            printf("ERROR: %s\n\n", get_error_message(ss_response.error_code));
        }
    }
    
    close(ss_socket);
}

void cmd_stream(char* filename) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_READ;
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0 || response.error_code != SUCCESS) {
        printf("ERROR: Failed to get storage server info\n\n");
        return;
    }
    
    char ss_ip[64];
    int ss_port;
    sscanf(response.data, "%[^|]|%d", ss_ip, &ss_port);
    
    // Connect to SS
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss_ip);
    ss_addr.sin_port = htons(ss_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        printf("ERROR: Failed to connect to storage server\n\n");
        close(ss_socket);
        return;
    }
    
    // Send STREAM to SS
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.command = CMD_STREAM;
    strncpy(ss_msg.username, username, MAX_USERNAME - 1);
    strncpy(ss_msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) >= 0) {
        if (ss_response.error_code == SUCCESS) {
            printf("\nStreaming: %s\n", filename);
            // Parse words separated by |WORD|
            char* data = ss_response.data;
            char* word_start = strstr(data, "|WORD|");
            while (word_start) {
                word_start += 6;  // Skip |WORD|
                char* word_end = strstr(word_start, "|WORD|");
                if (word_end) {
                    *word_end = '\0';
                }
                printf("%s ", word_start);
                fflush(stdout);
                usleep(100000);  // 0.1 second delay
                if (!word_end) break;
                word_start = word_end + 1;
            }
            printf("\n\n");
        } else {
            printf("ERROR: %s\n\n", get_error_message(ss_response.error_code));
        }
    }
    
    close(ss_socket);
}

void cmd_addaccess(char* filename, char* target_user) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_READ;
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0 || response.error_code != SUCCESS) {
        printf("ERROR: Failed to get storage server info\n\n");
        return;
    }
    
    char ss_ip[64];
    int ss_port;
    sscanf(response.data, "%[^|]|%d", ss_ip, &ss_port);
    
    // Connect to SS
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss_ip);
    ss_addr.sin_port = htons(ss_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        printf("ERROR: Failed to connect to storage server\n\n");
        close(ss_socket);
        return;
    }
    
    // Send ADDACCESS to SS
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.command = CMD_ADDACCESS;
    strncpy(ss_msg.username, username, MAX_USERNAME - 1);
    strncpy(ss_msg.filename, filename, MAX_FILENAME - 1);
    strncpy(ss_msg.data, target_user, BUFFER_SIZE - 1);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) >= 0) {
        if (ss_response.error_code == SUCCESS) {
            printf("Access granted to %s\n\n", target_user);
        } else {
            printf("ERROR: %s\n\n", get_error_message(ss_response.error_code));
        }
    }
    
    close(ss_socket);
}

void cmd_remaccess(char* filename, char* target_user) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_READ;
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0 || response.error_code != SUCCESS) {
        printf("ERROR: Failed to get storage server info\n\n");
        return;
    }
    
    char ss_ip[64];
    int ss_port;
    sscanf(response.data, "%[^|]|%d", ss_ip, &ss_port);
    
    // Connect to SS
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss_ip);
    ss_addr.sin_port = htons(ss_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        printf("ERROR: Failed to connect to storage server\n\n");
        close(ss_socket);
        return;
    }
    
    // Send REMACCESS to SS
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.command = CMD_REMACCESS;
    strncpy(ss_msg.username, username, MAX_USERNAME - 1);
    strncpy(ss_msg.filename, filename, MAX_FILENAME - 1);
    strncpy(ss_msg.data, target_user, BUFFER_SIZE - 1);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) >= 0) {
        if (ss_response.error_code == SUCCESS) {
            printf("Access revoked from %s\n\n", target_user);
        } else {
            printf("ERROR: %s\n\n", get_error_message(ss_response.error_code));
        }
    }
    
    close(ss_socket);
}

void cmd_exec(char* filename) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_EXEC;
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0) {
        printf("ERROR: Communication failed\n\n");
        return;
    }
    
    if (response.error_code == SUCCESS) {
        printf("\nExecution output:\n%s\n", response.data);
    } else {
        printf("ERROR: %s\n\n", get_error_message(response.error_code));
    }
}

void cmd_undo(char* filename) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_READ;
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0 || response.error_code != SUCCESS) {
        printf("ERROR: Failed to get storage server info\n\n");
        return;
    }
    
    char ss_ip[64];
    int ss_port;
    sscanf(response.data, "%[^|]|%d", ss_ip, &ss_port);
    
    // Connect to SS
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss_ip);
    ss_addr.sin_port = htons(ss_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        printf("ERROR: Failed to connect to storage server\n\n");
        close(ss_socket);
        return;
    }
    
    // Send UNDO to SS
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.command = CMD_UNDO;
    strncpy(ss_msg.username, username, MAX_USERNAME - 1);
    strncpy(ss_msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) >= 0) {
        if (ss_response.error_code == SUCCESS) {
            printf("Undo successful!\n\n");
        } else {
            printf("ERROR: %s\n\n", get_error_message(ss_response.error_code));
        }
    }
    
    close(ss_socket);
}

// BONUS: Create folder
void cmd_createfolder(char* foldername) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_READ;  // Get SS info
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, foldername, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0) {
        printf("ERROR: Communication failed\n");
        return;
    }
    
    // Parse SS info
    char ss_ip[64];
    int ss_port;
    if (sscanf(response.data, "%[^:]:%d", ss_ip, &ss_port) != 2) {
        printf("ERROR: Invalid server response\n");
        return;
    }
    
    // Connect to SS
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss_ip);
    ss_addr.sin_port = htons(ss_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        printf("ERROR: Cannot connect to storage server\n");
        close(ss_socket);
        return;
    }
    
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.msg_type = MSG_COMMAND;
    ss_msg.command = CMD_CREATEFOLDER;
    strncpy(ss_msg.username, username, MAX_USERNAME - 1);
    strncpy(ss_msg.filename, foldername, MAX_FILENAME - 1);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) >= 0) {
        if (ss_response.error_code == SUCCESS) {
            printf("Folder created successfully!\n\n");
        } else {
            printf("ERROR: %s\n\n", get_error_message(ss_response.error_code));
        }
    }
    
    close(ss_socket);
}

// BONUS: Move file to folder
void cmd_move(char* filename, char* foldername) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_READ;  // Get SS info
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0) {
        printf("ERROR: Communication failed\n");
        return;
    }
    
    // Parse SS info
    char ss_ip[64];
    int ss_port;
    if (sscanf(response.data, "%[^:]:%d", ss_ip, &ss_port) != 2) {
        printf("ERROR: Invalid server response\n");
        return;
    }
    
    // Connect to SS
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss_ip);
    ss_addr.sin_port = htons(ss_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        printf("ERROR: Cannot connect to storage server\n");
        close(ss_socket);
        return;
    }
    
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.msg_type = MSG_COMMAND;
    ss_msg.command = CMD_MOVE;
    strncpy(ss_msg.username, username, MAX_USERNAME - 1);
    snprintf(ss_msg.data, BUFFER_SIZE, "%s|%s", filename, foldername);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) >= 0) {
        if (ss_response.error_code == SUCCESS) {
            printf("File moved successfully!\n\n");
        } else {
            printf("ERROR: %s\n\n", get_error_message(ss_response.error_code));
        }
    }
    
    close(ss_socket);
}

// BONUS: View folder contents
void cmd_viewfolder(char* foldername) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_READ;  // Get SS info
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, foldername, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0) {
        printf("ERROR: Communication failed\n");
        return;
    }
    
    // Parse SS info
    char ss_ip[64];
    int ss_port;
    if (sscanf(response.data, "%[^:]:%d", ss_ip, &ss_port) != 2) {
        printf("ERROR: Invalid server response\n");
        return;
    }
    
    // Connect to SS
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss_ip);
    ss_addr.sin_port = htons(ss_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        printf("ERROR: Cannot connect to storage server\n");
        close(ss_socket);
        return;
    }
    
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.msg_type = MSG_COMMAND;
    ss_msg.command = CMD_VIEWFOLDER;
    strncpy(ss_msg.username, username, MAX_USERNAME - 1);
    strncpy(ss_msg.filename, foldername, MAX_FILENAME - 1);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) >= 0) {
        if (ss_response.error_code == SUCCESS) {
            printf("Contents of %s:\n%s\n\n", foldername, ss_response.data);
        } else {
            printf("ERROR: %s\n\n", get_error_message(ss_response.error_code));
        }
    }
    
    close(ss_socket);
}

// BONUS: Create checkpoint
void cmd_checkpoint(char* filename, char* tag) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_READ;  // Get SS info
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0) {
        printf("ERROR: Communication failed\n");
        return;
    }
    
    // Parse SS info
    char ss_ip[64];
    int ss_port;
    if (sscanf(response.data, "%[^:]:%d", ss_ip, &ss_port) != 2) {
        printf("ERROR: Invalid server response\n");
        return;
    }
    
    // Connect to SS
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss_ip);
    ss_addr.sin_port = htons(ss_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        printf("ERROR: Cannot connect to storage server\n");
        close(ss_socket);
        return;
    }
    
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.msg_type = MSG_COMMAND;
    ss_msg.command = CMD_CHECKPOINT;
    strncpy(ss_msg.username, username, MAX_USERNAME - 1);
    snprintf(ss_msg.data, BUFFER_SIZE, "%s|%s", filename, tag);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) >= 0) {
        if (ss_response.error_code == SUCCESS) {
            printf("Checkpoint created: %s\n\n", tag);
        } else {
            printf("ERROR: %s\n\n", get_error_message(ss_response.error_code));
        }
    }
    
    close(ss_socket);
}

// BONUS: View checkpoint
void cmd_viewcheckpoint(char* filename, char* tag) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_READ;  // Get SS info
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0) {
        printf("ERROR: Communication failed\n");
        return;
    }
    
    // Parse SS info
    char ss_ip[64];
    int ss_port;
    if (sscanf(response.data, "%[^:]:%d", ss_ip, &ss_port) != 2) {
        printf("ERROR: Invalid server response\n");
        return;
    }
    
    // Connect to SS
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss_ip);
    ss_addr.sin_port = htons(ss_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        printf("ERROR: Cannot connect to storage server\n");
        close(ss_socket);
        return;
    }
    
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.msg_type = MSG_COMMAND;
    ss_msg.command = CMD_VIEWCHECKPOINT;
    strncpy(ss_msg.username, username, MAX_USERNAME - 1);
    snprintf(ss_msg.data, BUFFER_SIZE, "%s|%s", filename, tag);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) >= 0) {
        if (ss_response.error_code == SUCCESS) {
            printf("Checkpoint content:\n%s\n\n", ss_response.data);
        } else {
            printf("ERROR: %s\n\n", get_error_message(ss_response.error_code));
        }
    }
    
    close(ss_socket);
}

// BONUS: Revert to checkpoint
void cmd_revert(char* filename, char* tag) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_READ;  // Get SS info
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0) {
        printf("ERROR: Communication failed\n");
        return;
    }
    
    // Parse SS info
    char ss_ip[64];
    int ss_port;
    if (sscanf(response.data, "%[^:]:%d", ss_ip, &ss_port) != 2) {
        printf("ERROR: Invalid server response\n");
        return;
    }
    
    // Connect to SS
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss_ip);
    ss_addr.sin_port = htons(ss_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        printf("ERROR: Cannot connect to storage server\n");
        close(ss_socket);
        return;
    }
    
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.msg_type = MSG_COMMAND;
    ss_msg.command = CMD_REVERT;
    strncpy(ss_msg.username, username, MAX_USERNAME - 1);
    snprintf(ss_msg.data, BUFFER_SIZE, "%s|%s", filename, tag);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) >= 0) {
        if (ss_response.error_code == SUCCESS) {
            printf("File reverted to checkpoint: %s\n\n", tag);
        } else {
            printf("ERROR: %s\n\n", get_error_message(ss_response.error_code));
        }
    }
    
    close(ss_socket);
}

// BONUS: List checkpoints
void cmd_listcheckpoints(char* filename) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_READ;  // Get SS info
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0) {
        printf("ERROR: Communication failed\n");
        return;
    }
    
    // Parse SS info
    char ss_ip[64];
    int ss_port;
    if (sscanf(response.data, "%[^:]:%d", ss_ip, &ss_port) != 2) {
        printf("ERROR: Invalid server response\n");
        return;
    }
    
    // Connect to SS
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss_ip);
    ss_addr.sin_port = htons(ss_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        printf("ERROR: Cannot connect to storage server\n");
        close(ss_socket);
        return;
    }
    
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.msg_type = MSG_COMMAND;
    ss_msg.command = CMD_LISTCHECKPOINTS;
    strncpy(ss_msg.username, username, MAX_USERNAME - 1);
    strncpy(ss_msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) >= 0) {
        if (ss_response.error_code == SUCCESS) {
            printf("Checkpoints for %s:\n%s\n\n", filename, ss_response.data);
        } else {
            printf("ERROR: %s\n\n", get_error_message(ss_response.error_code));
        }
    }
    
    close(ss_socket);
}

// BONUS: Request access
void cmd_requestaccess(char* filename) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_REQUESTACCESS;
    strncpy(msg.username, username, MAX_USERNAME - 1);
    strncpy(msg.filename, filename, MAX_FILENAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0) {
        printf("ERROR: Communication failed\n");
        return;
    }
    
    if (response.error_code == SUCCESS) {
        printf("%s\n\n", response.data);
    } else {
        printf("ERROR: %s\n\n", get_error_message(response.error_code));
    }
}

// BONUS: View pending requests
void cmd_viewrequests() {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_VIEWREQUESTS;
    strncpy(msg.username, username, MAX_USERNAME - 1);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0) {
        printf("ERROR: Communication failed\n");
        return;
    }
    
    if (response.error_code == SUCCESS) {
        printf("Pending Access Requests:\n%s\n\n", response.data);
    } else {
        printf("ERROR: %s\n\n", get_error_message(response.error_code));
    }
}

// BONUS: Approve request
void cmd_approverequest(char* filename, char* requester) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_APPROVEREQUEST;
    strncpy(msg.username, username, MAX_USERNAME - 1);
    snprintf(msg.data, BUFFER_SIZE, "%s|%s", filename, requester);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0) {
        printf("ERROR: Communication failed\n");
        return;
    }
    
    if (response.error_code == SUCCESS) {
        printf("%s\n\n", response.data);
    } else {
        printf("ERROR: %s\n\n", get_error_message(response.error_code));
    }
}

// BONUS: Deny request
void cmd_denyrequest(char* filename, char* requester) {
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_COMMAND;
    msg.command = CMD_DENYREQUEST;
    strncpy(msg.username, username, MAX_USERNAME - 1);
    snprintf(msg.data, BUFFER_SIZE, "%s|%s", filename, requester);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) < 0) {
        printf("ERROR: Communication failed\n");
        return;
    }
    
    if (response.error_code == SUCCESS) {
        printf("%s\n\n", response.data);
    } else {
        printf("ERROR: %s\n\n", get_error_message(response.error_code));
    }
}

void show_help() {
    printf("\nAvailable Commands:\n");
    printf("  VIEW                          List files\n");
    printf("  READ <filename>               Read file contents\n");
    printf("  CREATE <filename>             Create a new file\n");
    printf("  WRITE <filename> <sent#>      Write to file (enter edit mode)\n");
    printf("  DELETE <filename>             Delete a file\n");
    printf("  INFO <filename>               Show file metadata\n");
    printf("  FILEINFO <filename>           Show detailed file information\n");
    printf("  COPY <source> <destination>   Copy file to new name\n");
    printf("  STREAM <filename>             Stream file word-by-word\n");
    printf("  UNDO <filename>               Undo last write\n");
    printf("  ADDACCESS <filename> <user>   Grant user access\n");
    printf("  REMACCESS <filename> <user>   Revoke user access\n");
    printf("  EXEC <filename>               Execute file on server\n");
    printf("  LIST                          List all users\n");
    printf("\nBonus Commands:\n");
    printf("  CREATEFOLDER <name>           Create a folder\n");
    printf("  MOVE <file> <folder>          Move file to folder\n");
    printf("  VIEWFOLDER <name>             View folder contents\n");
    printf("  CHECKPOINT <file> <tag>       Save checkpoint\n");
    printf("  VIEWCHECKPOINT <file> <tag>   View checkpoint\n");
    printf("  REVERT <file> <tag>           Revert to checkpoint\n");
    printf("  LISTCHECKPOINTS <file>        List all checkpoints\n");
    printf("  REQUESTACCESS <file>          Request file access\n");
    printf("  VIEWREQUESTS                  View pending requests\n");
    printf("  APPROVEREQUEST <file> <user>  Approve access request\n");
    printf("  DENYREQUEST <file> <user>     Deny access request\n");
    printf("\n  HELP                          Show this help\n");
    printf("  EXIT                          Exit client\n\n");
}

void command_loop() {
    char line[512];
    
    show_help();
    
    while (running) {
        printf("%s> ", username);
        fflush(stdout);
        
        if (!fgets(line, sizeof(line), stdin)) {
            break;
        }
        
        // Remove newline
        line[strcspn(line, "\n")] = 0;
        
        if (strlen(line) == 0) {
            continue;
        }
        
        // Parse command
        char command[64];
        char arg1[MAX_FILENAME];
        char arg2[64];
        int args = sscanf(line, "%s %s %s", command, arg1, arg2);
        
        if (args < 1) {
            continue;
        }
        
        // Convert to uppercase
        for (int i = 0; command[i]; i++) {
            command[i] = toupper(command[i]);
        }
        
        if (strcmp(command, "EXIT") == 0 || strcmp(command, "QUIT") == 0) {
            break;
        } else if (strcmp(command, "HELP") == 0) {
            show_help();
        } else if (strcmp(command, "VIEW") == 0) {
            cmd_view(args > 1 ? arg1 : "");
        } else if (strcmp(command, "CREATE") == 0) {
            if (args < 2) {
                printf("Usage: CREATE <filename>\n\n");
            } else {
                cmd_create(arg1);
            }
        } else if (strcmp(command, "READ") == 0) {
            if (args < 2) {
                printf("Usage: READ <filename>\n\n");
            } else {
                cmd_read(arg1);
            }
        } else if (strcmp(command, "WRITE") == 0) {
            if (args < 3) {
                printf("Usage: WRITE <filename> <sentence_index>\n\n");
            } else {
                int sent_idx = atoi(arg2);
                cmd_write(arg1, sent_idx);
            }
        } else if (strcmp(command, "DELETE") == 0) {
            if (args < 2) {
                printf("Usage: DELETE <filename>\n\n");
            } else {
                cmd_delete(arg1);
            }
        } else if (strcmp(command, "INFO") == 0) {
            if (args < 2) {
                printf("Usage: INFO <filename>\n\n");
            } else {
                cmd_info(arg1);
            }
        } else if (strcmp(command, "FILEINFO") == 0) {
            if (args < 2) {
                printf("Usage: FILEINFO <filename>\n\n");
            } else {
                cmd_fileinfo(arg1);
            }
        } else if (strcmp(command, "COPY") == 0) {
            if (args < 3) {
                printf("Usage: COPY <source> <destination>\n\n");
            } else {
                cmd_copy(arg1, arg2);
            }
        } else if (strcmp(command, "STREAM") == 0) {
            if (args < 2) {
                printf("Usage: STREAM <filename>\n\n");
            } else {
                cmd_stream(arg1);
            }
        } else if (strcmp(command, "UNDO") == 0) {
            if (args < 2) {
                printf("Usage: UNDO <filename>\n\n");
            } else {
                cmd_undo(arg1);
            }
        } else if (strcmp(command, "ADDACCESS") == 0) {
            if (args < 3) {
                printf("Usage: ADDACCESS <filename> <username>\n\n");
            } else {
                cmd_addaccess(arg1, arg2);
            }
        } else if (strcmp(command, "REMACCESS") == 0) {
            if (args < 3) {
                printf("Usage: REMACCESS <filename> <username>\n\n");
            } else {
                cmd_remaccess(arg1, arg2);
            }
        } else if (strcmp(command, "EXEC") == 0) {
            if (args < 2) {
                printf("Usage: EXEC <filename>\n\n");
            } else {
                cmd_exec(arg1);
            }
        } else if (strcmp(command, "LIST") == 0) {
            cmd_list();
        } else if (strcmp(command, "CREATEFOLDER") == 0) {
            if (args < 2) {
                printf("Usage: CREATEFOLDER <foldername>\n\n");
            } else {
                cmd_createfolder(arg1);
            }
        } else if (strcmp(command, "MOVE") == 0) {
            if (args < 3) {
                printf("Usage: MOVE <filename> <foldername>\n\n");
            } else {
                cmd_move(arg1, arg2);
            }
        } else if (strcmp(command, "VIEWFOLDER") == 0) {
            if (args < 2) {
                printf("Usage: VIEWFOLDER <foldername>\n\n");
            } else {
                cmd_viewfolder(arg1);
            }
        } else if (strcmp(command, "CHECKPOINT") == 0) {
            if (args < 3) {
                printf("Usage: CHECKPOINT <filename> <tag>\n\n");
            } else {
                cmd_checkpoint(arg1, arg2);
            }
        } else if (strcmp(command, "VIEWCHECKPOINT") == 0) {
            if (args < 3) {
                printf("Usage: VIEWCHECKPOINT <filename> <tag>\n\n");
            } else {
                cmd_viewcheckpoint(arg1, arg2);
            }
        } else if (strcmp(command, "REVERT") == 0) {
            if (args < 3) {
                printf("Usage: REVERT <filename> <tag>\n\n");
            } else {
                cmd_revert(arg1, arg2);
            }
        } else if (strcmp(command, "LISTCHECKPOINTS") == 0) {
            if (args < 2) {
                printf("Usage: LISTCHECKPOINTS <filename>\n\n");
            } else {
                cmd_listcheckpoints(arg1);
            }
        } else if (strcmp(command, "REQUESTACCESS") == 0) {
            if (args < 2) {
                printf("Usage: REQUESTACCESS <filename>\n\n");
            } else {
                cmd_requestaccess(arg1);
            }
        } else if (strcmp(command, "VIEWREQUESTS") == 0) {
            cmd_viewrequests();
        } else if (strcmp(command, "APPROVEREQUEST") == 0) {
            if (args < 3) {
                printf("Usage: APPROVEREQUEST <filename> <username>\n\n");
            } else {
                cmd_approverequest(arg1, arg2);
            }
        } else if (strcmp(command, "DENYREQUEST") == 0) {
            if (args < 3) {
                printf("Usage: DENYREQUEST <filename> <username>\n\n");
            } else {
                cmd_denyrequest(arg1, arg2);
            }
        } else {
            printf("Unknown command: %s. Type HELP for available commands.\n\n", command);
        }
    }
}

int main() {
    signal(SIGINT, signal_handler_client);
    signal(SIGTERM, signal_handler_client);
    
    printf("=== Distributed File System Client ===\n\n");
    
    // Get username
    printf("Enter your username: ");
    fflush(stdout);
    if (!fgets(username, MAX_USERNAME, stdin)) {
        return 1;
    }
    username[strcspn(username, "\n")] = 0;
    
    if (strlen(username) == 0) {
        printf("Username cannot be empty\n");
        return 1;
    }
    
    printf("Welcome, %s!\n\n", username);
    
    // Connect to Name Server
    if (connect_to_nm() < 0) {
        return 1;
    }
    
    // Start command loop
    command_loop();
    
    printf("Goodbye!\n");
    cleanup_client();
    
    return 0;
}
