#include "../include/common.h"
#include "../include/sentence_parser.h"
#include "../include/file_locking.h"
#include <signal.h>
#include <fcntl.h>
#include <glib.h>

#define SS_CLIENT_PORT 7000
#define SS_ID "SS1"

int client_server_socket = -1;
int running = 1;

// Connection pool for clients
typedef struct {
    int fd;
    time_t last_used;
    bool in_use;
    pthread_mutex_t mutex;
} connection_t;

static connection_t* connection_pool = NULL;
static int connection_pool_size = 0;
static pthread_mutex_t connection_pool_mutex = PTHREAD_MUTEX_INITIALIZER;

// Cleanup function for connection pool
static void cleanup_connection_pool() {
    if (connection_pool) {
        for (int i = 0; i < connection_pool_size; i++) {
            if (connection_pool[i].fd >= 0) {
                close(connection_pool[i].fd);
                pthread_mutex_destroy(&connection_pool[i].mutex);
            }
        }
        g_free(connection_pool);
        connection_pool = NULL;
        connection_pool_size = 0;
    }
}

void cleanup_ss() {
    running = 0;
    
    // Close client server socket
    if (client_server_socket >= 0) {
        close(client_server_socket);
        client_server_socket = -1;
    }
    
    // Clean up connection pool
    cleanup_connection_pool();
    
    // Clean up file locking system
    file_locking_cleanup();
    
    // Clean up any other resources
    pthread_mutex_destroy(&connection_pool_mutex);
    
    log_message("STORAGE_SERVER", "INFO", "Storage Server shutdown complete");
}

// Initialize the connection pool with a given size
static int init_connection_pool(int size) {
    if (size <= 0) {
        log_message("CONN_POOL", "ERROR", "Invalid connection pool size: %d", size);
        return -1;
    }
    
    pthread_mutex_lock(&connection_pool_mutex);
    
    // Clean up existing pool if any
    cleanup_connection_pool();
    
    // Allocate new pool
    connection_pool = g_malloc0_n(size, sizeof(connection_t));
    if (!connection_pool) {
        log_message("CONN_POOL", "ERROR", "Failed to allocate connection pool");
        pthread_mutex_unlock(&connection_pool_mutex);
        return -1;
    }
    
    connection_pool_size = size;
    
    // Initialize pool entries
    for (int i = 0; i < size; i++) {
        connection_pool[i].fd = -1;
        connection_pool[i].in_use = false;
        connection_pool[i].last_used = 0;
        if (pthread_mutex_init(&connection_pool[i].mutex, NULL) != 0) {
            log_message("CONN_POOL", "ERROR", "Failed to initialize mutex for connection %d", i);
            // Clean up any initialized mutexes
            while (--i >= 0) {
                pthread_mutex_destroy(&connection_pool[i].mutex);
            }
            g_free(connection_pool);
            connection_pool = NULL;
            connection_pool_size = 0;
            pthread_mutex_unlock(&connection_pool_mutex);
            return -1;
        }
    }
    
    log_message("CONN_POOL", "INFO", "Initialized connection pool with %d connections", size);
    pthread_mutex_unlock(&connection_pool_mutex);
    return 0;
}

// Get an available connection from the pool
static connection_t* get_connection() {
    connection_t* conn = NULL;
    
    pthread_mutex_lock(&connection_pool_mutex);
    
    // Try to find an available connection
    for (int i = 0; i < connection_pool_size; i++) {
        if (!connection_pool[i].in_use) {
            conn = &connection_pool[i];
            conn->in_use = true;
            conn->last_used = time(NULL);
            pthread_mutex_lock(&conn->mutex);
            break;
        }
    }
    
    pthread_mutex_unlock(&connection_pool_mutex);
    
    if (!conn) {
        log_message("CONN_POOL", "WARNING", "No available connections in pool");
    }
    
    return conn;
}

// Release a connection back to the pool
static void release_connection(connection_t* conn) {
    if (!conn) return;
    
    // Reset connection state
    conn->last_used = time(NULL);
    conn->in_use = false;
    
    // Unlock the connection's mutex
    pthread_mutex_unlock(&conn->mutex);
}

// Clean up idle connections that haven't been used for more than timeout_sec seconds
static void cleanup_idle_connections(int timeout_sec) {
    if (timeout_sec <= 0) return;
    
    time_t now = time(NULL);
    int cleaned = 0;
    
    pthread_mutex_lock(&connection_pool_mutex);
    
    for (int i = 0; i < connection_pool_size; i++) {
        if (connection_pool[i].in_use && 
            (now - connection_pool[i].last_used) > timeout_sec) {
            
            pthread_mutex_lock(&connection_pool[i].mutex);
            
            // Close the socket if it's still open
            if (connection_pool[i].fd >= 0) {
                close(connection_pool[i].fd);
                connection_pool[i].fd = -1;
            }
            
            connection_pool[i].in_use = false;
            connection_pool[i].last_used = 0;
            
            pthread_mutex_unlock(&connection_pool[i].mutex);
            
            cleaned++;
        }
    }
    
    if (cleaned > 0) {
        log_message("CONN_POOL", "DEBUG", "Cleaned up %d idle connections", cleaned);
    }
    
    pthread_mutex_unlock(&connection_pool_mutex);
}

void signal_handler_ss(int signum) {
    log_message("STORAGE_SERVER", "INFO", "Received signal %d, shutting down...", signum);
    cleanup_ss();
    exit(0);
}

// Set socket options for keepalive
static int set_keepalive(int sockfd, int keepalive, int keepidle, int keepintvl, int keepcnt) {
    int optval = keepalive;
    if (setsockopt(sockfd, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval)) < 0) {
        return -1;
    }
    
    if (keepalive) {
        // TCP_KEEPIDLE: time (in seconds) the connection needs to remain idle before TCP starts sending keepalive probes
        if (setsockopt(sockfd, IPPROTO_TCP, TCP_KEEPIDLE, &keepidle, sizeof(keepidle)) < 0) {
            return -1;
        }
        
        // TCP_KEEPINTVL: time (in seconds) between individual keepalive probes
        if (setsockopt(sockfd, IPPROTO_TCP, TCP_KEEPINTVL, &keepintvl, sizeof(keepintvl)) < 0) {
            return -1;
        }
        
        // TCP_KEEPCNT: maximum number of keepalive probes TCP should send before dropping the connection
        if (setsockopt(sockfd, IPPROTO_TCP, TCP_KEEPCNT, &keepcnt, sizeof(keepcnt)) < 0) {
            return -1;
        }
    }
    
    return 0;
}

// Thread function to handle NM registration and heartbeats
static void* nm_heartbeat_thread(void* arg) {
    (void)arg; // Unused parameter
    
    while (running) {
        int nm_socket = socket(AF_INET, SOCK_STREAM, 0);
        if (nm_socket < 0) {
            log_message("NM_HEARTBEAT", "ERROR", "Failed to create socket for NM: %s", strerror(errno));
            sleep(5); // Wait before retrying
            continue;
        }
        
        // Set socket options
        int optval = 1;
        setsockopt(nm_socket, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
        
        // Set keepalive options
        if (set_keepalive(nm_socket, 1, 30, 10, 3) < 0) {
            log_message("NM_HEARTBEAT", "WARNING", "Failed to set keepalive options: %s", strerror(errno));
        }
        
        struct sockaddr_in nm_addr;
        memset(&nm_addr, 0, sizeof(nm_addr));
        nm_addr.sin_family = AF_INET;
        nm_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
        nm_addr.sin_port = htons(NM_PORT);
        
        // Set connection timeout
        struct timeval timeout;
        timeout.tv_sec = 5;  // 5 second timeout
        timeout.tv_usec = 0;
        setsockopt(nm_socket, SOL_SOCKET, SO_SNDTIMEO, (char *)&timeout, sizeof(timeout));
        setsockopt(nm_socket, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout, sizeof(timeout));
        
        // Connect to NM
        if (connect(nm_socket, (struct sockaddr*)&nm_addr, sizeof(nm_addr)) < 0) {
            log_message("NM_HEARTBEAT", "ERROR", "Failed to connect to NM: %s", strerror(errno));
            close(nm_socket);
            sleep(5); // Wait before retrying
            continue;
        }
        
        log_message("NM_HEARTBEAT", "INFO", "Connected to Naming Server");
        
        // Send registration message
        Message msg;
        memset(&msg, 0, sizeof(Message));
        msg.msg_type = MSG_REGISTER_SS;
        snprintf(msg.data, BUFFER_SIZE, "%s|127.0.0.1|%d|%d", SS_ID, 6000, SS_CLIENT_PORT);
        
        if (send(nm_socket, &msg, sizeof(Message), 0) < 0) {
            log_message("NM_HEARTBEAT", "ERROR", "Failed to send registration to NM: %s", strerror(errno));
            close(nm_socket);
            sleep(5); // Wait before retrying
            continue;
        }
        
        log_message("NM_HEARTBEAT", "INFO", "Successfully registered with Naming Server");
        
        // Heartbeat loop
        while (running) {
            // Send heartbeat every 30 seconds
            sleep(30);
            
            Message hb_msg;
            memset(&hb_msg, 0, sizeof(Message));
            hb_msg.msg_type = MSG_HEARTBEAT;
            snprintf(hb_msg.data, BUFFER_SIZE, "%s|ALIVE|%d", SS_ID, getpid());
            
            if (send(nm_socket, &hb_msg, sizeof(Message), 0) < 0) {
                log_message("NM_HEARTBEAT", "ERROR", "Failed to send heartbeat to NM: %s", strerror(errno));
                break; // Exit heartbeat loop to reconnect
            }
            
            // Wait for acknowledgment (with timeout handled by SO_RCVTIMEO)
            Message ack_msg;
            ssize_t bytes_received = recv(nm_socket, &ack_msg, sizeof(Message), 0);
            
            if (bytes_received <= 0) {
                if (bytes_received == 0) {
                    log_message("NM_HEARTBEAT", "WARNING", "Naming Server closed the connection");
                } else {
                    log_message("NM_HEARTBEAT", "ERROR", "Error receiving heartbeat ack: %s", strerror(errno));
                }
                break; // Exit heartbeat loop to reconnect
            }
            
            if (ack_msg.msg_type == MSG_ACK) {
                log_message("NM_HEARTBEAT", "DEBUG", "Received heartbeat ack from Naming Server");
            }
        }
        
        // Clean up
        close(nm_socket);
    }
    
    return NULL;
}

void register_with_nm() {
    // Initialize the connection pool
    if (init_connection_pool(MAX_CLIENTS) < 0) {
        log_message("STORAGE_SERVER", "ERROR", "Failed to initialize connection pool");
        return;
    }
    
    // Initialize file locking system
    file_locking_init();
    
    // Create heartbeat thread
    pthread_t heartbeat_tid;
    if (pthread_create(&heartbeat_tid, NULL, nm_heartbeat_thread, NULL) != 0) {
        log_message("STORAGE_SERVER", "ERROR", "Failed to create heartbeat thread: %s", strerror(errno));
        return;
    }
    
    // Detach the thread so we don't need to join it
    pthread_detach(heartbeat_tid);
    
    log_message("STORAGE_SERVER", "INFO", "Started Naming Server registration and heartbeat thread");
}

// Validate filename to prevent directory traversal and other security issues
static int validate_filename(const char* filename) {
    if (!filename || *filename == '\0' || strstr(filename, "..") || strchr(filename, '/')) {
        return -1;
    }
    
    // Check for invalid characters
    const char* invalid_chars = "<>:\"|?*\\";
    if (strpbrk(filename, invalid_chars) != NULL) {
        return -1;
    }
    
    // Check maximum length
    size_t len = strlen(filename);
    if (len >= MAX_FILENAME || len == 0) {
        return -1;
    }
    
    return 0;
}

// Ensure the data directory exists
static int ensure_data_directory() {
    static int dir_checked = 0;
    if (dir_checked) return 0;
    
    struct stat st = {0};
    if (stat("data", &st) == -1) {
        if (mkdir("data", 0755) == -1 && errno != EEXIST) {
            log_message("FILE_OPS", "ERROR", "Failed to create data directory: %s", strerror(errno));
            return -1;
        }
    }
    
    if (stat("data/files", &st) == -1) {
        if (mkdir("data/files", 0755) == -1 && errno != EEXIST) {
            log_message("FILE_OPS", "ERROR", "Failed to create data/files directory: %s", strerror(errno));
            return -1;
        }
    }
    
    dir_checked = 1;
    return 0;
}

int load_file_content(const char* filename, char* content, int max_len) {
    // Validate input parameters
    if (!filename || !content || max_len <= 0) {
        log_message("FILE_OPS", "ERROR", "Invalid parameters to load_file_content");
        return -1;
    }
    
    // Validate filename
    if (validate_filename(filename) != 0) {
        log_message("FILE_OPS", "ERROR", "Invalid filename: %s", filename);
        return -1;
    }
    
    // Ensure data directory exists
    if (ensure_data_directory() != 0) {
        return -1;
    }
    
    char filepath[MAX_PATH];
    snprintf(filepath, MAX_PATH, "data/files/%s", filename);
    
    // Acquire read lock
    if (file_read_lock(filepath) != 0) {
        log_message("FILE_OPS", "ERROR", "Failed to acquire read lock for %s", filename);
        return -1;
    }
    
    FILE* fp = NULL;
    int result = -1;
    
    do {
        fp = fopen(filepath, "r");
        if (!fp) {
            log_message("FILE_OPS", "ERROR", "Failed to open file %s: %s", filepath, strerror(errno));
            break;
        }
        
        // Get file size
        if (fseek(fp, 0, SEEK_END) != 0) {
            log_message("FILE_OPS", "ERROR", "Failed to seek to end of file %s", filepath);
            break;
        }
        
        long file_size = ftell(fp);
        if (file_size < 0) {
            log_message("FILE_OPS", "ERROR", "Failed to get file size for %s", filepath);
            break;
        }
        
        // Check if file is too large for our buffer
        if (file_size >= max_len - 1) {
            log_message("FILE_OPS", "ERROR", "File %s is too large (%ld bytes, max %d)", 
                       filepath, file_size, max_len - 1);
            break;
        }
        
        // Read file content
        rewind(fp);
        size_t bytes_read = fread(content, 1, file_size, fp);
        if (ferror(fp)) {
            log_message("FILE_OPS", "ERROR", "Error reading file %s", filepath);
            break;
        }
        
        content[bytes_read] = '\0';
        result = (int)bytes_read;
        
    } while (0);
    
    // Clean up
    if (fp) {
        fclose(fp);
    }
    
    // Release the lock
    file_unlock(filepath);
    
    if (result >= 0) {
        log_message("FILE_OPS", "DEBUG", "Successfully read %d bytes from %s", result, filename);
    }
    
    return result;
}

// Save content to a temporary file and then atomically rename it
static int atomic_write_file(const char* filepath, const char* content, size_t content_len) {
    char tmp_path[MAX_PATH + 16];
    snprintf(tmp_path, sizeof(tmp_path), "%s.XXXXXX.tmp", filepath);
    
    // Create a temporary file with a unique name
    int fd = mkstemps(tmp_path, 4); // 4 is the length of ".tmp"
    if (fd < 0) {
        log_message("FILE_OPS", "ERROR", "Failed to create temporary file for %s: %s", 
                   filepath, strerror(errno));
        return -1;
    }
    
    FILE* fp = fdopen(fd, "w");
    if (!fp) {
        log_message("FILE_OPS", "ERROR", "Failed to open temporary file %s: %s", 
                   tmp_path, strerror(errno));
        close(fd);
        unlink(tmp_path); // Clean up
        return -1;
    }
    
    // Write content to temporary file
    size_t written = fwrite(content, 1, content_len, fp);
    if (ferror(fp) || written != content_len) {
        log_message("FILE_OPS", "ERROR", "Failed to write to temporary file %s: %s", 
                   tmp_path, strerror(errno));
        fclose(fp);
        unlink(tmp_path);
        return -1;
    }
    
    // Ensure all data is written to disk
    if (fflush(fp) != 0 || fsync(fileno(fp)) != 0) {
        log_message("FILE_OPS", "ERROR", "Failed to sync temporary file %s: %s", 
                   tmp_path, strerror(errno));
        fclose(fp);
        unlink(tmp_path);
        return -1;
    }
    
    fclose(fp);
    
    // Atomically rename the temporary file to the target file
    if (rename(tmp_path, filepath) != 0) {
        log_message("FILE_OPS", "ERROR", "Failed to rename %s to %s: %s", 
                   tmp_path, filepath, strerror(errno));
        unlink(tmp_path);
        return -1;
    }
    
    // Ensure the directory is synced to disk (important for durability)
    char dirpath[MAX_PATH];
    strncpy(dirpath, filepath, MAX_PATH);
    char* last_slash = strrchr(dirpath, '/');
    if (last_slash) {
        *last_slash = '\0';
        int dir_fd = open(dirpath, O_RDONLY);
        if (dir_fd >= 0) {
            fsync(dir_fd);
            close(dir_fd);
        }
    }
    
    return 0;
}

int save_file_content(const char* filename, const char* content) {
    // Validate input parameters
    if (!filename || !content) {
        log_message("FILE_OPS", "ERROR", "Invalid parameters to save_file_content");
        return -1;
    }
    
    // Validate filename
    if (validate_filename(filename) != 0) {
        log_message("FILE_OPS", "ERROR", "Invalid filename: %s", filename);
        return -1;
    }
    
    // Ensure data directory exists
    if (ensure_data_directory() != 0) {
        return -1;
    }
    
    char filepath[MAX_PATH];
    snprintf(filepath, MAX_PATH, "data/files/%s", filename);
    
    // Acquire write lock
    if (file_write_lock(filepath) != 0) {
        log_message("FILE_OPS", "ERROR", "Failed to acquire write lock for %s", filename);
        return -1;
    }
    
    int result = -1;
    size_t content_len = strlen(content);
    
    // Use atomic write to prevent partial writes
    if (atomic_write_file(filepath, content, content_len) == 0) {
        log_message("FILE_OPS", "INFO", "Successfully saved %zu bytes to %s", 
                   content_len, filename);
        result = 0;
    } else {
        log_message("FILE_OPS", "ERROR", "Failed to save content to %s", filename);
    }
    
    // Release the lock
    file_unlock(filepath);
    
    return result;
}

// Load entire file content into dynamically allocated string
char* load_file(const char* filename) {
    // Validate input parameters
    if (!filename) {
        log_message("FILE_OPS", "ERROR", "NULL filename in load_file");
        return NULL;
    }
    
    // First get the file size
    char filepath[MAX_PATH];
    snprintf(filepath, MAX_PATH, "data/files/%s", filename);
    
    // Use stat to get file size without opening the file
    struct stat st;
    if (stat(filepath, &st) != 0) {
        log_message("FILE_OPS", "ERROR", "Failed to stat file %s: %s", 
                   filename, strerror(errno));
        return NULL;
    }
    
    // Allocate buffer with extra byte for null terminator
    char* content = (char*)malloc(st.st_size + 1);
    if (!content) {
        log_message("FILE_OPS", "ERROR", "Failed to allocate %ld bytes for file %s", 
                   (long)st.st_size + 1, filename);
        return NULL;
    }
    
    // Use load_file_content to read the file with proper locking
    int bytes_read = load_file_content(filename, content, st.st_size + 1);
    if (bytes_read < 0) {
        log_message("FILE_OPS", "ERROR", "Failed to read file %s", filename);
        free(content);
        return NULL;
    }
    
    // Ensure null termination
    content[bytes_read] = '\0';
    
    return content;
}

// Save content to file with proper error handling
int save_file(const char* filename, const char* content) {
    if (!filename || !content) {
        log_message("FILE_OPS", "ERROR", "Invalid parameters to save_file");
        return -1;
    }
    
    // Use save_file_content which handles all the error checking and atomic writes
    return save_file_content(filename, content);
}

int load_metadata(const char* filename, FileInfo* info, ACLEntry acl[], int* acl_count) {
    char metapath[MAX_PATH];
    snprintf(metapath, MAX_PATH, "data/metadata/%s.meta", filename);
    
    FILE* fp = fopen(metapath, "r");
    if (!fp) {
        return -1;
    }
    
    char line[512];
    *acl_count = 0;
    
    while (fgets(line, sizeof(line), fp)) {
        if (strncmp(line, "owner:", 6) == 0) {
            sscanf(line + 6, "%s", info->owner);
        } else if (strncmp(line, "created:", 8) == 0) {
            sscanf(line + 8, "%ld", &info->created);
        } else if (strncmp(line, "modified:", 9) == 0) {
            sscanf(line + 9, "%ld", &info->modified);
        } else if (strncmp(line, "accessed:", 9) == 0) {
            sscanf(line + 9, "%ld", &info->accessed);
        } else if (strncmp(line, "accessed_by:", 12) == 0) {
            sscanf(line + 12, "%s", info->last_accessed_by);
        } else if (strncmp(line, "words:", 6) == 0) {
            sscanf(line + 6, "%d", &info->word_count);
        } else if (strncmp(line, "chars:", 6) == 0) {
            sscanf(line + 6, "%d", &info->char_count);
        } else if (strncmp(line, "acl:", 4) == 0) {
            char username[MAX_USERNAME];
            char perm;
            if (sscanf(line + 4, "%[^:]:%c", username, &perm) == 2) {
                strncpy(acl[*acl_count].username, username, MAX_USERNAME - 1);
                acl[*acl_count].permission = (perm == 'W') ? PERM_WRITE : PERM_READ;
                (*acl_count)++;
            }
        }
    }
    
    fclose(fp);
    return 0;
}

int save_metadata(const char* filename, FileInfo* info, ACLEntry acl[], int acl_count) {
    char metapath[MAX_PATH];
    snprintf(metapath, MAX_PATH, "data/metadata/%s.meta", filename);
    
    FILE* fp = fopen(metapath, "w");
    if (!fp) {
        return -1;
    }
    
    fprintf(fp, "owner:%s\n", info->owner);
    fprintf(fp, "created:%ld\n", info->created);
    fprintf(fp, "modified:%ld\n", info->modified);
    fprintf(fp, "accessed:%ld\n", info->accessed);
    fprintf(fp, "accessed_by:%s\n", info->last_accessed_by);
    fprintf(fp, "words:%d\n", info->word_count);
    fprintf(fp, "chars:%d\n", info->char_count);
    
    for (int i = 0; i < acl_count; i++) {
        fprintf(fp, "acl:%s:%c\n", acl[i].username,
                acl[i].permission == PERM_WRITE ? 'W' : 'R');
    }
    
    fclose(fp);
    return 0;
}

int check_access(const char* filename, const char* username, int required_perm) {
    FileInfo info;
    ACLEntry acl[MAX_ACL_ENTRIES];
    int acl_count = 0;
    
    if (load_metadata(filename, &info, acl, &acl_count) < 0) {
        return 0;
    }
    
    // Owner always has full access
    if (strcmp(info.owner, username) == 0) {
        return 1;
    }
    
    // Check ACL
    for (int i = 0; i < acl_count; i++) {
        if (strcmp(acl[i].username, username) == 0) {
            if (required_perm == PERM_READ) {
                return acl[i].permission >= PERM_READ;
            } else if (required_perm == PERM_WRITE) {
                return acl[i].permission == PERM_WRITE;
            }
        }
    }
    
    return 0;
}

void handle_create_file(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    char filepath[MAX_PATH];
    snprintf(filepath, MAX_PATH, "data/files/%s", msg->filename);
    
    // Check if file exists
    if (access(filepath, F_OK) == 0) {
        response->error_code = ERR_FILE_EXISTS;
        snprintf(response->data, BUFFER_SIZE, "File already exists");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Create empty file
    FILE* fp = fopen(filepath, "w");
    if (!fp) {
        response->error_code = ERR_INTERNAL;
        snprintf(response->data, BUFFER_SIZE, "Failed to create file");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    fclose(fp);
    
    // Create metadata
    FileInfo info;
    strncpy(info.filename, msg->filename, MAX_FILENAME - 1);
    strncpy(info.owner, msg->username, MAX_USERNAME - 1);
    info.created = time(NULL);
    info.modified = info.created;
    info.accessed = info.created;
    strncpy(info.last_accessed_by, msg->username, MAX_USERNAME - 1);
    info.word_count = 0;
    info.char_count = 0;
    
    ACLEntry acl[MAX_ACL_ENTRIES];
    int acl_count = 0;
    
    save_metadata(msg->filename, &info, acl, acl_count);
    
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "File %s created", msg->filename);
    
    log_message("STORAGE_SERVER", "INFO", "File created: %s by %s", msg->filename, msg->username);
    
    pthread_mutex_unlock(&file_mutex);
}

void handle_read_file(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    if (!check_access(msg->filename, msg->username, PERM_READ)) {
        response->error_code = ERR_UNAUTHORIZED;
        snprintf(response->data, BUFFER_SIZE, "No read access");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    char content[BUFFER_SIZE];
    if (load_file_content(msg->filename, content, BUFFER_SIZE) < 0) {
        response->error_code = ERR_FILE_NOT_FOUND;
        snprintf(response->data, BUFFER_SIZE, "File not found");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Update access time
    FileInfo info;
    ACLEntry acl[MAX_ACL_ENTRIES];
    int acl_count = 0;
    
    if (load_metadata(msg->filename, &info, acl, &acl_count) == 0) {
        info.accessed = time(NULL);
        strncpy(info.last_accessed_by, msg->username, MAX_USERNAME - 1);
        save_metadata(msg->filename, &info, acl, acl_count);
    }
    
    response->error_code = SUCCESS;
    strncpy(response->data, content, BUFFER_SIZE - 1);
    response->data_len = strlen(content);
    
    log_message("STORAGE_SERVER", "INFO", "File read: %s by %s", msg->filename, msg->username);
    
    pthread_mutex_unlock(&file_mutex);
}

void handle_write_commit(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    if (!check_access(msg->filename, msg->username, PERM_WRITE)) {
        response->error_code = ERR_UNAUTHORIZED;
        snprintf(response->data, BUFFER_SIZE, "No write access");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Load current content
    char content[BUFFER_SIZE];
    int content_len = load_file_content(msg->filename, content, BUFFER_SIZE);
    if (content_len < 0) {
        content[0] = '\0';
    }
    
    // Save undo copy
    char undo_path[MAX_PATH];
    snprintf(undo_path, MAX_PATH, "data/undo/%s.undo", msg->filename);
    FILE* undo_fp = fopen(undo_path, "w");
    if (undo_fp) {
        fputs(content, undo_fp);
        fclose(undo_fp);
    }
    
    // Parse write data: sentence_index|word_index|word|word_index|word|...
    int sentence_index;
    char* data_ptr = msg->data;
    sscanf(data_ptr, "%d|", &sentence_index);
    data_ptr = strchr(data_ptr, '|') + 1;
    
    // Parse sentences
    char sentences[MAX_SENTENCES][MAX_SENTENCE_LENGTH];
    int num_sentences = parse_sentences(content, sentences, MAX_SENTENCES);
    
    // Validate sentence index (allow creating new sentence at end)
    if (sentence_index < 0 || sentence_index > num_sentences) {
        response->error_code = ERR_INVALID_INDEX;
        snprintf(response->data, BUFFER_SIZE, "Invalid sentence index %d (max: %d)", 
                 sentence_index, num_sentences);
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Get target sentence (or create new one)
    char sentence[MAX_SENTENCE_LENGTH];
    if (sentence_index < num_sentences) {
        strncpy(sentence, sentences[sentence_index], MAX_SENTENCE_LENGTH - 1);
    } else {
        sentence[0] = '\0';
    }
    
    // Apply all edits
    while (*data_ptr != '\0') {
        int word_index;
        char word[MAX_WORD_LENGTH];
        
        if (sscanf(data_ptr, "%d|%[^|]|", &word_index, word) != 2) {
            break;
        }
        
        char new_sentence[MAX_SENTENCE_LENGTH];
        if (insert_word(sentence, word_index, word, new_sentence, MAX_SENTENCE_LENGTH) != SUCCESS) {
            response->error_code = ERR_INVALID_INDEX;
            snprintf(response->data, BUFFER_SIZE, "Invalid word index");
            pthread_mutex_unlock(&file_mutex);
            return;
        }
        
        strncpy(sentence, new_sentence, MAX_SENTENCE_LENGTH - 1);
        
        // Move to next edit
        data_ptr = strchr(data_ptr, '|') + 1;  // Skip word_index
        data_ptr = strchr(data_ptr, '|') + 1;  // Skip word
    }
    
    // Check if new sentence creates multiple sentences (due to delimiters)
    char new_sentences[MAX_SENTENCES][MAX_SENTENCE_LENGTH];
    int new_sent_count = parse_sentences(sentence, new_sentences, MAX_SENTENCES);
    
    // Replace old sentence with new sentence(s)
    char result_sentences[MAX_SENTENCES][MAX_SENTENCE_LENGTH];
    int result_count = 0;
    
    // Copy sentences before
    for (int i = 0; i < sentence_index && i < num_sentences; i++) {
        strncpy(result_sentences[result_count++], sentences[i], MAX_SENTENCE_LENGTH - 1);
    }
    
    // Add new sentences
    for (int i = 0; i < new_sent_count; i++) {
        strncpy(result_sentences[result_count++], new_sentences[i], MAX_SENTENCE_LENGTH - 1);
    }
    
    // Copy sentences after
    for (int i = sentence_index + 1; i < num_sentences; i++) {
        strncpy(result_sentences[result_count++], sentences[i], MAX_SENTENCE_LENGTH - 1);
    }
    
    // Rebuild full text
    char new_content[BUFFER_SIZE];
    rebuild_text(result_sentences, result_count, new_content, BUFFER_SIZE);
    
    // Save file
    save_file_content(msg->filename, new_content);
    
    // Update metadata
    FileInfo info;
    ACLEntry acl[MAX_ACL_ENTRIES];
    int acl_count = 0;
    
    if (load_metadata(msg->filename, &info, acl, &acl_count) == 0) {
        info.modified = time(NULL);
        get_text_stats(new_content, &info.word_count, &info.char_count, NULL);
        save_metadata(msg->filename, &info, acl, acl_count);
    }
    
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "Write successful");
    
    log_message("STORAGE_SERVER", "INFO", "File written: %s by %s (sentence %d)",
               msg->filename, msg->username, sentence_index);
    
    pthread_mutex_unlock(&file_mutex);
}

void handle_delete_file(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    FileInfo info;
    ACLEntry acl[MAX_ACL_ENTRIES];
    int acl_count = 0;
    
    if (load_metadata(msg->filename, &info, acl, &acl_count) < 0) {
        response->error_code = ERR_FILE_NOT_FOUND;
        snprintf(response->data, BUFFER_SIZE, "File not found");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    if (strcmp(info.owner, msg->username) != 0) {
        response->error_code = ERR_UNAUTHORIZED;
        snprintf(response->data, BUFFER_SIZE, "Only owner can delete");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Delete file
    char filepath[MAX_PATH];
    snprintf(filepath, MAX_PATH, "data/files/%s", msg->filename);
    unlink(filepath);
    
    // Delete metadata
    char metapath[MAX_PATH];
    snprintf(metapath, MAX_PATH, "data/metadata/%s.meta", msg->filename);
    unlink(metapath);
    
    // Delete undo
    char undopath[MAX_PATH];
    snprintf(undopath, MAX_PATH, "data/undo/%s.undo", msg->filename);
    unlink(undopath);
    
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "File deleted");
    
    log_message("STORAGE_SERVER", "INFO", "File deleted: %s by %s", msg->filename, msg->username);
    
    pthread_mutex_unlock(&file_mutex);
}

void handle_undo(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    if (!check_access(msg->filename, msg->username, PERM_WRITE)) {
        response->error_code = ERR_UNAUTHORIZED;
        snprintf(response->data, BUFFER_SIZE, "No write access");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Load undo content
    char undo_path[MAX_PATH];
    snprintf(undo_path, MAX_PATH, "data/undo/%s.undo", msg->filename);
    
    char undo_content[BUFFER_SIZE];
    FILE* undo_fp = fopen(undo_path, "r");
    if (!undo_fp) {
        response->error_code = ERR_INVALID_PARAMETERS;
        snprintf(response->data, BUFFER_SIZE, "No undo history");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    int bytes = fread(undo_content, 1, BUFFER_SIZE - 1, undo_fp);
    undo_content[bytes] = '\0';
    fclose(undo_fp);
    
    // Save current as new undo
    char current_content[BUFFER_SIZE];
    load_file_content(msg->filename, current_content, BUFFER_SIZE);
    
    undo_fp = fopen(undo_path, "w");
    if (undo_fp) {
        fputs(current_content, undo_fp);
        fclose(undo_fp);
    }
    
    // Restore undo content
    save_file_content(msg->filename, undo_content);
    
    // Update metadata
    FileInfo info;
    ACLEntry acl[MAX_ACL_ENTRIES];
    int acl_count = 0;
    
    if (load_metadata(msg->filename, &info, acl, &acl_count) == 0) {
        info.modified = time(NULL);
        get_text_stats(undo_content, &info.word_count, &info.char_count, NULL);
        save_metadata(msg->filename, &info, acl, acl_count);
    }
    
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "Undo successful");
    
    log_message("STORAGE_SERVER", "INFO", "Undo: %s by %s", msg->filename, msg->username);
    
    pthread_mutex_unlock(&file_mutex);
}

void handle_info(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    if (!check_access(msg->filename, msg->username, PERM_READ)) {
        response->error_code = ERR_UNAUTHORIZED;
        snprintf(response->data, BUFFER_SIZE, "No read access");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    FileInfo info;
    ACLEntry acl[MAX_ACL_ENTRIES];
    int acl_count = 0;
    
    if (load_metadata(msg->filename, &info, acl, &acl_count) < 0) {
        response->error_code = ERR_FILE_NOT_FOUND;
        snprintf(response->data, BUFFER_SIZE, "File not found");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Format metadata info
    char created_str[64], modified_str[64];
    strftime(created_str, sizeof(created_str), "%Y-%m-%d %H:%M:%S", localtime(&info.created));
    strftime(modified_str, sizeof(modified_str), "%Y-%m-%d %H:%M:%S", localtime(&info.modified));
    
    // Count sentences from content
    char content[BUFFER_SIZE];
    int sentence_count = 0;
    if (load_file_content(msg->filename, content, BUFFER_SIZE) >= 0) {
        get_text_stats(content, NULL, NULL, &sentence_count);
    }
    
    snprintf(response->data, BUFFER_SIZE,
             "File: %s\nOwner: %s\nCreated: %s\nModified: %s\nWords: %d\nCharacters: %d\nSentences: %d\nACL: ",
             msg->filename, info.owner, created_str, modified_str, 
             info.word_count, info.char_count, sentence_count);
    
    if (acl_count == 0) {
        strcat(response->data, "none");
    } else {
        for (int i = 0; i < acl_count; i++) {
            strcat(response->data, acl[i].username);
            if (i < acl_count - 1) strcat(response->data, ", ");
        }
    }
    
    response->error_code = SUCCESS;
    log_message("STORAGE_SERVER", "INFO", "Info: %s by %s", msg->filename, msg->username);
    
    pthread_mutex_unlock(&file_mutex);
}

void handle_stream(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    if (!check_access(msg->filename, msg->username, PERM_READ)) {
        response->error_code = ERR_UNAUTHORIZED;
        snprintf(response->data, BUFFER_SIZE, "No read access");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    char content[BUFFER_SIZE];
    if (load_file_content(msg->filename, content, BUFFER_SIZE) < 0) {
        response->error_code = ERR_FILE_NOT_FOUND;
        snprintf(response->data, BUFFER_SIZE, "File not found");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Parse into words
    char words[MAX_WORDS][MAX_WORD_LENGTH];
    int word_count = parse_words(content, words, MAX_WORDS);
    
    // Stream word by word with delimiter |WORD|
    response->data[0] = '\0';
    for (int i = 0; i < word_count && i < 100; i++) {  // Limit to 100 words for buffer
        strcat(response->data, "|WORD|");
        strcat(response->data, words[i]);
    }
    
    response->error_code = SUCCESS;
    log_message("STORAGE_SERVER", "INFO", "Stream: %s by %s (%d words)", 
                msg->filename, msg->username, word_count);
    
    pthread_mutex_unlock(&file_mutex);
}

void handle_add_access(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    // Only owner can modify ACL
    FileInfo info;
    ACLEntry acl[MAX_ACL_ENTRIES];
    int acl_count = 0;
    
    if (load_metadata(msg->filename, &info, acl, &acl_count) < 0) {
        response->error_code = ERR_FILE_NOT_FOUND;
        snprintf(response->data, BUFFER_SIZE, "File not found");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    if (strcmp(info.owner, msg->username) != 0) {
        response->error_code = ERR_UNAUTHORIZED;
        snprintf(response->data, BUFFER_SIZE, "Only owner can modify access");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Parse username from data
    char target_user[MAX_USERNAME];
    sscanf(msg->data, "%s", target_user);
    
    // Check if user already in ACL
    for (int i = 0; i < acl_count; i++) {
        if (strcmp(acl[i].username, target_user) == 0) {
            response->error_code = ERR_INVALID_PARAMETERS;
            snprintf(response->data, BUFFER_SIZE, "User already has access");
            pthread_mutex_unlock(&file_mutex);
            return;
        }
    }
    
    // Add to ACL
    if (acl_count >= MAX_ACL_ENTRIES) {
        response->error_code = ERR_INVALID_PARAMETERS;
        snprintf(response->data, BUFFER_SIZE, "ACL full");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    strncpy(acl[acl_count].username, target_user, MAX_USERNAME - 1);
    acl[acl_count].permission = PERM_READ | PERM_WRITE;
    acl_count++;
    
    save_metadata(msg->filename, &info, acl, acl_count);
    
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "Access granted to %s", target_user);
    log_message("STORAGE_SERVER", "INFO", "AddAccess: %s granted to %s by %s", 
                msg->filename, target_user, msg->username);
    
    pthread_mutex_unlock(&file_mutex);
}

void handle_rem_access(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    // Only owner can modify ACL
    FileInfo info;
    ACLEntry acl[MAX_ACL_ENTRIES];
    int acl_count = 0;
    
    if (load_metadata(msg->filename, &info, acl, &acl_count) < 0) {
        response->error_code = ERR_FILE_NOT_FOUND;
        snprintf(response->data, BUFFER_SIZE, "File not found");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    if (strcmp(info.owner, msg->username) != 0) {
        response->error_code = ERR_UNAUTHORIZED;
        snprintf(response->data, BUFFER_SIZE, "Only owner can modify access");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Parse username from data
    char target_user[MAX_USERNAME];
    sscanf(msg->data, "%s", target_user);
    
    // Find and remove from ACL
    int found = 0;
    for (int i = 0; i < acl_count; i++) {
        if (strcmp(acl[i].username, target_user) == 0) {
            // Shift remaining entries
            for (int j = i; j < acl_count - 1; j++) {
                acl[j] = acl[j + 1];
            }
            acl_count--;
            found = 1;
            break;
        }
    }
    
    if (!found) {
        response->error_code = ERR_INVALID_PARAMETERS;
        snprintf(response->data, BUFFER_SIZE, "User not in ACL");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    save_metadata(msg->filename, &info, acl, acl_count);
    
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "Access revoked from %s", target_user);
    log_message("STORAGE_SERVER", "INFO", "RemAccess: %s revoked from %s by %s", 
                msg->filename, target_user, msg->username);
    
    pthread_mutex_unlock(&file_mutex);
}

// FILEINFO: Get detailed file information
void handle_fileinfo(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    if (!check_access(msg->filename, msg->username, PERM_READ)) {
        response->error_code = ERR_UNAUTHORIZED;
        snprintf(response->data, BUFFER_SIZE, "No read access");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    FileInfo info;
    ACLEntry acl[MAX_ACL_ENTRIES];
    int acl_count = 0;
    
    if (load_metadata(msg->filename, &info, acl, &acl_count) < 0) {
        response->error_code = ERR_FILE_NOT_FOUND;
        snprintf(response->data, BUFFER_SIZE, "File not found");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Get file size
    char filepath[MAX_PATH];
    snprintf(filepath, MAX_PATH, "data/files/%s", msg->filename);
    struct stat st;
    long file_size = 0;
    if (stat(filepath, &st) == 0) {
        file_size = st.st_size;
    }
    
    // Format timestamps
    char created_str[64], modified_str[64], accessed_str[64];
    strftime(created_str, sizeof(created_str), "%Y-%m-%d %H:%M:%S", localtime(&info.created));
    strftime(modified_str, sizeof(modified_str), "%Y-%m-%d %H:%M:%S", localtime(&info.modified));
    strftime(accessed_str, sizeof(accessed_str), "%Y-%m-%d %H:%M:%S", localtime(&info.accessed));
    
    // Count sentences from content
    char content[BUFFER_SIZE];
    int sentence_count = 0;
    if (load_file_content(msg->filename, content, BUFFER_SIZE) >= 0) {
        get_text_stats(content, NULL, NULL, &sentence_count);
    }
    
    // Build response with complete file information
    snprintf(response->data, BUFFER_SIZE,
             "=== File Information ===\n"
             "Filename: %s\n"
             "Owner: %s\n"
             "Size: %ld bytes\n"
             "Created: %s\n"
             "Modified: %s\n"
             "Last Accessed: %s by %s\n"
             "Words: %d\n"
             "Characters: %d\n"
             "Sentences: %d\n"
             "Storage Server: %s\n"
             "Access Control List: ",
             msg->filename, info.owner, file_size, 
             created_str, modified_str, accessed_str, info.last_accessed_by,
             info.word_count, info.char_count, sentence_count, info.ss_id);
    
    if (acl_count == 0) {
        strcat(response->data, "none\n");
    } else {
        char acl_str[256] = "";
        for (int i = 0; i < acl_count; i++) {
            strcat(acl_str, acl[i].username);
            strcat(acl_str, " (");
            strcat(acl_str, acl[i].permission == PERM_READ ? "read" : "write");
            strcat(acl_str, ")");
            if (i < acl_count - 1) strcat(acl_str, ", ");
        }
        strcat(response->data, acl_str);
        strcat(response->data, "\n");
    }
    
    response->error_code = SUCCESS;
    log_message("STORAGE_SERVER", "INFO", "FileInfo: %s by %s", msg->filename, msg->username);
    
    pthread_mutex_unlock(&file_mutex);
}

// COPY: Copy file to new name
void handle_copy(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    // Parse: source|destination
    char source[MAX_FILENAME], destination[MAX_FILENAME];
    if (sscanf(msg->data, "%[^|]|%s", source, destination) != 2) {
        response->error_code = ERR_INVALID_PARAMETERS;
        strcpy(response->data, "Invalid parameters. Use: COPY source destination");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Check read access on source
    if (!check_access(source, msg->username, PERM_READ)) {
        response->error_code = ERR_UNAUTHORIZED;
        snprintf(response->data, BUFFER_SIZE, "No read access to source file");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Load source metadata and content
    FileInfo src_info;
    ACLEntry src_acl[MAX_ACL_ENTRIES];
    int src_acl_count = 0;
    
    if (load_metadata(source, &src_info, src_acl, &src_acl_count) < 0) {
        response->error_code = ERR_FILE_NOT_FOUND;
        snprintf(response->data, BUFFER_SIZE, "Source file not found");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Check if destination exists
    FileInfo dest_info;
    ACLEntry dest_acl[MAX_ACL_ENTRIES];
    int dest_acl_count = 0;
    if (load_metadata(destination, &dest_info, dest_acl, &dest_acl_count) == 0) {
        response->error_code = ERR_FILE_EXISTS;
        snprintf(response->data, BUFFER_SIZE, "Destination file already exists");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Load source content
    char content[BUFFER_SIZE];
    if (load_file_content(source, content, BUFFER_SIZE) < 0) {
        response->error_code = ERR_INTERNAL;
        snprintf(response->data, BUFFER_SIZE, "Failed to read source file");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Create destination file with current user as owner
    FileInfo new_info;
    strncpy(new_info.filename, destination, MAX_FILENAME);
    strncpy(new_info.owner, msg->username, MAX_USERNAME);
    strncpy(new_info.ss_id, src_info.ss_id, sizeof(new_info.ss_id));
    new_info.created = time(NULL);
    new_info.modified = new_info.created;
    new_info.accessed = new_info.created;
    strncpy(new_info.last_accessed_by, msg->username, MAX_USERNAME);
    new_info.word_count = src_info.word_count;
    new_info.char_count = src_info.char_count;
    
    // Save destination content
    if (save_file_content(destination, content) < 0) {
        response->error_code = ERR_INTERNAL;
        snprintf(response->data, BUFFER_SIZE, "Failed to write destination file");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Save destination metadata (empty ACL - only owner access)
    ACLEntry empty_acl[MAX_ACL_ENTRIES];
    save_metadata(destination, &new_info, empty_acl, 0);
    
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "File copied: %s -> %s", source, destination);
    log_message("STORAGE_SERVER", "INFO", "Copy: %s -> %s by %s", 
                source, destination, msg->username);
    
    pthread_mutex_unlock(&file_mutex);
}

// BONUS: Create folder
void handle_create_folder(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    char folderpath[MAX_PATH];
    snprintf(folderpath, MAX_PATH, "data/files/%s", msg->filename);
    
    if (mkdir(folderpath, 0755) == 0) {
        response->error_code = SUCCESS;
        snprintf(response->data, BUFFER_SIZE, "Folder created: %s", msg->filename);
        log_message("STORAGE_SERVER", "INFO", "Folder created: %s by %s", 
                    msg->filename, msg->username);
    } else {
        response->error_code = ERR_INTERNAL;
        snprintf(response->data, BUFFER_SIZE, "Failed to create folder: %s", strerror(errno));
    }
    
    pthread_mutex_unlock(&file_mutex);
}

// BONUS: Move file to folder
void handle_move_file(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    // Parse: filename|foldername
    char filename[MAX_FILENAME], foldername[MAX_FILENAME];
    if (sscanf(msg->data, "%[^|]|%s", filename, foldername) != 2) {
        response->error_code = ERR_INVALID_PARAMETERS;
        strcpy(response->data, "Invalid parameters");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    char oldpath[MAX_PATH], newpath[MAX_PATH];
    snprintf(oldpath, MAX_PATH, "data/files/%s", filename);
    snprintf(newpath, MAX_PATH, "data/files/%s/%s", foldername, filename);
    
    if (rename(oldpath, newpath) == 0) {
        // Also move metadata and undo files
        char old_meta[MAX_PATH], new_meta[MAX_PATH];
        char old_undo[MAX_PATH], new_undo[MAX_PATH];
        snprintf(old_meta, MAX_PATH, "data/metadata/%s.meta", filename);
        snprintf(new_meta, MAX_PATH, "data/metadata/%s/%s.meta", foldername, filename);
        snprintf(old_undo, MAX_PATH, "data/undo/%s.undo", filename);
        snprintf(new_undo, MAX_PATH, "data/undo/%s/%s.undo", foldername, filename);
        
        rename(old_meta, new_meta);
        rename(old_undo, new_undo);
        
        response->error_code = SUCCESS;
        snprintf(response->data, BUFFER_SIZE, "File moved to folder: %s", foldername);
        log_message("STORAGE_SERVER", "INFO", "File moved: %s to %s by %s", 
                    filename, foldername, msg->username);
    } else {
        response->error_code = ERR_INTERNAL;
        snprintf(response->data, BUFFER_SIZE, "Failed to move file: %s", strerror(errno));
    }
    
    pthread_mutex_unlock(&file_mutex);
}

// BONUS: View folder contents
void handle_view_folder(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    char folderpath[MAX_PATH];
    snprintf(folderpath, MAX_PATH, "data/files/%s", msg->filename);
    
    DIR* dir = opendir(folderpath);
    if (!dir) {
        response->error_code = ERR_FILE_NOT_FOUND;
        strcpy(response->data, "Folder not found");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    char result[BUFFER_SIZE] = "";
    struct dirent* entry;
    int count = 0;
    
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
            if (count > 0) strcat(result, "\n");
            strcat(result, entry->d_name);
            count++;
        }
    }
    closedir(dir);
    
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "%s", result);
    log_message("STORAGE_SERVER", "INFO", "ViewFolder: %s by %s (%d items)", 
                msg->filename, msg->username, count);
    
    pthread_mutex_unlock(&file_mutex);
}

// BONUS: Save checkpoint
void handle_checkpoint(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    // Parse: filename|tag
    char filename[MAX_FILENAME], tag[64];
    if (sscanf(msg->data, "%[^|]|%s", filename, tag) != 2) {
        response->error_code = ERR_INVALID_PARAMETERS;
        strcpy(response->data, "Invalid parameters");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    char filepath[MAX_PATH];
    snprintf(filepath, MAX_PATH, "data/files/%s", filename);
    
    char* content = load_file(filename);
    if (!content) {
        response->error_code = ERR_FILE_NOT_FOUND;
        strcpy(response->data, "File not found");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    // Save checkpoint
    char checkpoint_path[MAX_PATH];
    snprintf(checkpoint_path, MAX_PATH, "data/checkpoints/%s_%s.ckpt", filename, tag);
    
    FILE* fp = fopen(checkpoint_path, "w");
    if (fp) {
        fprintf(fp, "%ld\n", time(NULL));
        fprintf(fp, "%s", content);
        fclose(fp);
        
        response->error_code = SUCCESS;
        snprintf(response->data, BUFFER_SIZE, "Checkpoint created: %s", tag);
        log_message("STORAGE_SERVER", "INFO", "Checkpoint: %s tag=%s by %s", 
                    filename, tag, msg->username);
    } else {
        response->error_code = ERR_INTERNAL;
        strcpy(response->data, "Failed to create checkpoint");
    }
    
    free(content);
    pthread_mutex_unlock(&file_mutex);
}

// BONUS: View checkpoint
void handle_view_checkpoint(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    // Parse: filename|tag
    char filename[MAX_FILENAME], tag[64];
    if (sscanf(msg->data, "%[^|]|%s", filename, tag) != 2) {
        response->error_code = ERR_INVALID_PARAMETERS;
        strcpy(response->data, "Invalid parameters");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    char checkpoint_path[MAX_PATH];
    snprintf(checkpoint_path, MAX_PATH, "data/checkpoints/%s_%s.ckpt", filename, tag);
    
    FILE* fp = fopen(checkpoint_path, "r");
    if (!fp) {
        response->error_code = ERR_FILE_NOT_FOUND;
        strcpy(response->data, "Checkpoint not found");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    char timestamp_str[64];
    fgets(timestamp_str, sizeof(timestamp_str), fp);
    
    char content[BUFFER_SIZE];
    size_t bytes = fread(content, 1, BUFFER_SIZE - 1, fp);
    content[bytes] = '\0';
    fclose(fp);
    
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "%s", content);
    log_message("STORAGE_SERVER", "INFO", "ViewCheckpoint: %s tag=%s by %s", 
                filename, tag, msg->username);
    
    pthread_mutex_unlock(&file_mutex);
}

// BONUS: Revert to checkpoint
void handle_revert_checkpoint(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    // Parse: filename|tag
    char filename[MAX_FILENAME], tag[64];
    if (sscanf(msg->data, "%[^|]|%s", filename, tag) != 2) {
        response->error_code = ERR_INVALID_PARAMETERS;
        strcpy(response->data, "Invalid parameters");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    char checkpoint_path[MAX_PATH];
    snprintf(checkpoint_path, MAX_PATH, "data/checkpoints/%s_%s.ckpt", filename, tag);
    
    FILE* fp = fopen(checkpoint_path, "r");
    if (!fp) {
        response->error_code = ERR_FILE_NOT_FOUND;
        strcpy(response->data, "Checkpoint not found");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    char timestamp_str[64];
    fgets(timestamp_str, sizeof(timestamp_str), fp);
    
    char content[BUFFER_SIZE];
    size_t bytes = fread(content, 1, BUFFER_SIZE - 1, fp);
    content[bytes] = '\0';
    fclose(fp);
    
    // Save current content to undo
    char* current = load_file(filename);
    if (current) {
        char undo_path[MAX_PATH];
        snprintf(undo_path, MAX_PATH, "data/undo/%s.undo", filename);
        FILE* undo_fp = fopen(undo_path, "w");
        if (undo_fp) {
            fprintf(undo_fp, "%s", current);
            fclose(undo_fp);
        }
        free(current);
    }
    
    // Restore checkpoint content
    save_file(filename, content);
    
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "Reverted to checkpoint: %s", tag);
    log_message("STORAGE_SERVER", "INFO", "Revert: %s to tag=%s by %s", 
                filename, tag, msg->username);
    
    pthread_mutex_unlock(&file_mutex);
}

// BONUS: List checkpoints
void handle_list_checkpoints(Message* msg, Message* response) {
    pthread_mutex_lock(&file_mutex);
    
    DIR* dir = opendir("data/checkpoints");
    if (!dir) {
        response->error_code = ERR_INTERNAL;
        strcpy(response->data, "No checkpoints directory");
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
    char result[BUFFER_SIZE] = "";
    struct dirent* entry;
    int count = 0;
    char prefix[MAX_FILENAME + 1];
    snprintf(prefix, sizeof(prefix), "%s_", msg->filename);
    
    while ((entry = readdir(dir)) != NULL) {
        if (strncmp(entry->d_name, prefix, strlen(prefix)) == 0) {
            // Extract tag from filename
            char* tag_start = entry->d_name + strlen(prefix);
            char* tag_end = strstr(tag_start, ".ckpt");
            if (tag_end) {
                if (count > 0) strcat(result, "\n");
                strncat(result, tag_start, tag_end - tag_start);
                count++;
            }
        }
    }
    closedir(dir);
    
    response->error_code = SUCCESS;
    if (count == 0) {
        strcpy(response->data, "No checkpoints found");
    } else {
        snprintf(response->data, BUFFER_SIZE, "%s", result);
    }
    log_message("STORAGE_SERVER", "INFO", "ListCheckpoints: %s by %s (%d found)", 
                msg->filename, msg->username, count);
    
    pthread_mutex_unlock(&file_mutex);
}

void* handle_ss_client(void* arg) {
    int client_socket = *(int*)arg;
    free(arg);
    
    Message msg, response;
    
    while (running) {
        memset(&msg, 0, sizeof(Message));
        memset(&response, 0, sizeof(Message));
        
        if (receive_message(client_socket, &msg) < 0) {
            break;
        }
        
        response.msg_type = MSG_RESPONSE;
        
        switch (msg.command) {
            case CMD_CREATE:
                handle_create_file(&msg, &response);
                break;
            case CMD_READ:
                handle_read_file(&msg, &response);
                break;
            case CMD_WRITE_COMMIT:
                handle_write_commit(&msg, &response);
                break;
            case CMD_DELETE:
                handle_delete_file(&msg, &response);
                break;
            case CMD_UNDO:
                handle_undo(&msg, &response);
                break;
            case CMD_COPY:
                handle_copy(&msg, &response);
                break;
            case CMD_FILEINFO:
                handle_fileinfo(&msg, &response);
                break;
            case CMD_INFO:
                handle_info(&msg, &response);
                break;
            case CMD_STREAM:
                handle_stream(&msg, &response);
                break;
            case CMD_ADDACCESS:
                handle_add_access(&msg, &response);
                break;
            case CMD_REMACCESS:
                handle_rem_access(&msg, &response);
                break;
            case CMD_CREATEFOLDER:
                handle_create_folder(&msg, &response);
                break;
            case CMD_MOVE:
                handle_move_file(&msg, &response);
                break;
            case CMD_VIEWFOLDER:
                handle_view_folder(&msg, &response);
                break;
            case CMD_CHECKPOINT:
                handle_checkpoint(&msg, &response);
                break;
            case CMD_VIEWCHECKPOINT:
                handle_view_checkpoint(&msg, &response);
                break;
            case CMD_REVERT:
                handle_revert_checkpoint(&msg, &response);
                break;
            case CMD_LISTCHECKPOINTS:
                handle_list_checkpoints(&msg, &response);
                break;
            default:
                response.error_code = ERR_INVALID_COMMAND;
        }
        
        send_message(client_socket, &response);
    }
    
    close(client_socket);
    return NULL;
}

int main() {
    signal(SIGINT, signal_handler_ss);
    signal(SIGTERM, signal_handler_ss);
    
    pthread_mutex_init(&file_mutex, NULL);
    
    // Create required directories
    mkdir("data", 0755);
    mkdir("data/files", 0755);
    mkdir("data/metadata", 0755);
    mkdir("data/undo", 0755);
    mkdir("data/checkpoints", 0755);
    mkdir("logs", 0755);
    
    log_message("STORAGE_SERVER", "INFO", "Storage Server %s starting", SS_ID);
    
    // Register with Name Server
    register_with_nm();
    
    // Start client server
    client_server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (client_server_socket < 0) {
        log_message("STORAGE_SERVER", "ERROR", "Failed to create socket");
        return 1;
    }
    
    int opt = 1;
    setsockopt(client_server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(SS_CLIENT_PORT);
    
    if (bind(client_server_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        log_message("STORAGE_SERVER", "ERROR", "Failed to bind: %s", strerror(errno));
        return 1;
    }
    
    if (listen(client_server_socket, MAX_CLIENTS) < 0) {
        log_message("STORAGE_SERVER", "ERROR", "Failed to listen: %s", strerror(errno));
        return 1;
    }
    
    log_message("STORAGE_SERVER", "INFO", "Storage Server listening on port %d", SS_CLIENT_PORT);
    printf("Storage Server %s started on port %d\n", SS_ID, SS_CLIENT_PORT);
    
    while (running) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        int* client_socket = malloc(sizeof(int));
        *client_socket = accept(client_server_socket, (struct sockaddr*)&client_addr, &client_len);
        
        if (*client_socket < 0) {
            if (running) {
                log_message("STORAGE_SERVER", "ERROR", "Accept failed");
            }
            free(client_socket);
            continue;
        }
        
        log_message("STORAGE_SERVER", "INFO", "Client connected from %s:%d",
                   inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
        
        pthread_t thread;
        pthread_create(&thread, NULL, handle_ss_client, client_socket);
        pthread_detach(thread);
    }
    
    cleanup_ss();
    return 0;
}
