#include "../include/common.h"
#include "../include/hashmap.h"
#include <signal.h>
#include <limits.h>

// Global state
HashMap* file_registry;  // filename -> FileInfo*
HashMap* user_registry;  // username -> UserInfo*
HashMap* ss_registry;    // ss_id -> StorageServerInfo*
HashMap* sentence_locks; // "filename:index" -> SentenceLock*
LRUCache* file_cache;    // Recent file lookups
HashMap* access_requests;  // BONUS: "filename:username" -> AccessRequest*
pthread_mutex_t registry_lock;

int server_socket = -1;
int running = 1;

void cleanup() {
    running = 0;
    if (server_socket >= 0) {
        close(server_socket);
    }
    
    if (file_registry) hashmap_destroy(file_registry);
    if (user_registry) hashmap_destroy(user_registry);
    if (ss_registry) hashmap_destroy(ss_registry);
    if (sentence_locks) hashmap_destroy(sentence_locks);
    if (file_cache) lru_destroy(file_cache);
    if (access_requests) hashmap_destroy(access_requests);
    
    pthread_mutex_destroy(&registry_lock);
    
    log_message("NAME_SERVER", "INFO", "Server shutdown complete");
}

void signal_handler(int signum) {
    log_message("NAME_SERVER", "INFO", "Received signal %d, shutting down...", signum);
    cleanup();
    exit(0);
}

void init_name_server() {
    file_registry = hashmap_create();
    user_registry = hashmap_create();
    ss_registry = hashmap_create();
    sentence_locks = hashmap_create();
    file_cache = lru_create(100);
    access_requests = hashmap_create();  // BONUS
    
    pthread_mutex_init(&registry_lock, NULL);
    
    // Create logs directory if it doesn't exist
    mkdir("logs", 0755);
    
    log_message("NAME_SERVER", "INFO", "Name Server initialized");
}

void load_file_registry() {
    FILE* fp = fopen("data/file_registry.txt", "r");
    if (!fp) {
        log_message("NAME_SERVER", "INFO", "No existing file registry found");
        return;
    }
    
    char line[1024];
    while (fgets(line, sizeof(line), fp)) {
        FileInfo* info = (FileInfo*)malloc(sizeof(FileInfo));
        char accessed_by[MAX_USERNAME];
        
        sscanf(line, "%[^|]|%[^|]|%[^|]|%ld|%ld|%ld|%[^|]|%d|%d",
               info->filename, info->owner, info->ss_id,
               &info->created, &info->modified, &info->accessed,
               accessed_by, &info->word_count, &info->char_count);
        
        strncpy(info->last_accessed_by, accessed_by, MAX_USERNAME - 1);
        
        hashmap_put(file_registry, info->filename, info);
        
        log_message("NAME_SERVER", "INFO", "Loaded file: %s (owner: %s, SS: %s)",
                   info->filename, info->owner, info->ss_id);
    }
    
    fclose(fp);
}

void save_file_registry() {
    FILE* fp = fopen("data/file_registry.txt", "w");
    if (!fp) {
        log_message("NAME_SERVER", "ERROR", "Failed to save file registry");
        return;
    }
    
    char keys[1000][MAX_FILENAME];
    int count = 0;
    hashmap_get_keys(file_registry, keys, &count);
    
    for (int i = 0; i < count; i++) {
        FileInfo* info = (FileInfo*)hashmap_get(file_registry, keys[i]);
        if (info) {
            fprintf(fp, "%s|%s|%s|%ld|%ld|%ld|%s|%d|%d\n",
                   info->filename, info->owner, info->ss_id,
                   info->created, info->modified, info->accessed,
                   info->last_accessed_by, info->word_count, info->char_count);
        }
    }
    
    fclose(fp);
    log_message("NAME_SERVER", "INFO", "File registry saved (%d files)", count);
}

void handle_register_ss(Message* msg, Message* response) {
    StorageServerInfo* ss_info = (StorageServerInfo*)malloc(sizeof(StorageServerInfo));
    
    // Parse SS registration data from msg->data
    sscanf(msg->data, "%[^|]|%[^|]|%d|%d",
           ss_info->ss_id, ss_info->ip, &ss_info->nm_port, &ss_info->client_port);
    
    ss_info->connected = 1;
    ss_info->last_heartbeat = time(NULL);
    ss_info->file_count = 0;
    memset(ss_info->replica_ss_id, 0, sizeof(ss_info->replica_ss_id));
    
    // BONUS: Assign replica SS (simple: next SS in registry)
    char keys[1000][MAX_FILENAME];
    int ss_count = 0;
    hashmap_get_keys(ss_registry, keys, &ss_count);
    
    if (ss_count > 0) {
        // Assign previous SS as replica
        StorageServerInfo* prev_ss = (StorageServerInfo*)hashmap_get(ss_registry, keys[ss_count - 1]);
        if (prev_ss) {
            strncpy(ss_info->replica_ss_id, prev_ss->ss_id, sizeof(ss_info->replica_ss_id) - 1);
            strncpy(prev_ss->replica_ss_id, ss_info->ss_id, sizeof(prev_ss->replica_ss_id) - 1);
        }
    }
    
    hashmap_put(ss_registry, ss_info->ss_id, ss_info);
    
    log_message("NAME_SERVER", "INFO", "Storage Server registered: %s at %s:%d (replica: %s)",
               ss_info->ss_id, ss_info->ip, ss_info->client_port, 
               ss_info->replica_ss_id[0] ? ss_info->replica_ss_id : "none");
    
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "SS %s registered successfully", ss_info->ss_id);
}

void handle_register_user(Message* msg, Message* response) {
    UserInfo* user_info = (UserInfo*)malloc(sizeof(UserInfo));
    
    strncpy(user_info->username, msg->username, MAX_USERNAME - 1);
    sscanf(msg->data, "%[^|]|%d", user_info->ip, &user_info->port);
    user_info->registered = time(NULL);
    
    hashmap_put(user_registry, user_info->username, user_info);
    
    log_message("NAME_SERVER", "INFO", "User registered: %s from %s:%d",
               user_info->username, user_info->ip, user_info->port);
    
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "User %s registered", user_info->username);
}

void handle_view(Message* msg, Message* response) {
    char keys[1000][MAX_FILENAME];
    int count = 0;
    hashmap_get_keys(file_registry, keys, &count);
    
    // Build file list
    response->data[0] = '\0';
    int pos = 0;
    
    for (int i = 0; i < count && pos < BUFFER_SIZE - MAX_FILENAME - 10; i++) {
        FileInfo* info = (FileInfo*)hashmap_get(file_registry, keys[i]);
        if (info) {
            pos += snprintf(response->data + pos, BUFFER_SIZE - pos,
                          "%s|%s|%d|%d|",
                          info->filename, info->owner, info->word_count, info->char_count);
        }
    }
    
    response->error_code = SUCCESS;
    log_message("NAME_SERVER", "INFO", "VIEW command: %d files listed for %s",
               count, msg->username);
}

void handle_create(Message* msg, Message* response) {
    if (hashmap_contains(file_registry, msg->filename)) {
        response->error_code = ERR_FILE_EXISTS;
        snprintf(response->data, BUFFER_SIZE, "File %s already exists", msg->filename);
        return;
    }
    
    // Select a storage server with load balancing (round-robin based on file count)
    char ss_keys[100][64];
    int ss_count = 0;
    hashmap_get_keys(ss_registry, (char(*)[MAX_FILENAME])ss_keys, &ss_count);
    
    if (ss_count == 0) {
        response->error_code = ERR_NO_STORAGE_SERVERS;
        snprintf(response->data, BUFFER_SIZE, "No storage servers available");
        return;
    }
    
    // Load balancing: Select SS with minimum file count
    StorageServerInfo* selected_ss = NULL;
    int min_files = INT_MAX;
    
    for (int i = 0; i < ss_count; i++) {
        StorageServerInfo* ss = (StorageServerInfo*)hashmap_get(ss_registry, ss_keys[i]);
        if (ss && ss->connected && ss->file_count < min_files) {
            min_files = ss->file_count;
            selected_ss = ss;
        }
    }
    
    if (!selected_ss) {
        // Fallback to first available
        selected_ss = (StorageServerInfo*)hashmap_get(ss_registry, ss_keys[0]);
    }
    
    // Increment file count for load balancing
    selected_ss->file_count++;
    
    // Create file info
    FileInfo* info = (FileInfo*)malloc(sizeof(FileInfo));
    strncpy(info->filename, msg->filename, MAX_FILENAME - 1);
    strncpy(info->owner, msg->username, MAX_USERNAME - 1);
    strncpy(info->ss_id, selected_ss->ss_id, 63);
    info->created = time(NULL);
    info->modified = info->created;
    info->accessed = info->created;
    strncpy(info->last_accessed_by, msg->username, MAX_USERNAME - 1);
    info->word_count = 0;
    info->char_count = 0;
    
    hashmap_put(file_registry, msg->filename, info);
    save_file_registry();
    
    // Return SS info for client to connect
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "%s|%d", selected_ss->ip, selected_ss->client_port);
    
    log_message("NAME_SERVER", "INFO", "File created: %s (owner: %s, SS: %s, load: %d files)",
               msg->filename, msg->username, selected_ss->ss_id, selected_ss->file_count);
}

void handle_read(Message* msg, Message* response) {
    FileInfo* info = (FileInfo*)hashmap_get(file_registry, msg->filename);
    
    if (!info) {
        response->error_code = ERR_FILE_NOT_FOUND;
        snprintf(response->data, BUFFER_SIZE, "File %s not found", msg->filename);
        return;
    }
    
    // Update cache
    FileInfo* cached_info = (FileInfo*)malloc(sizeof(FileInfo));
    memcpy(cached_info, info, sizeof(FileInfo));
    lru_put(file_cache, msg->filename, cached_info);
    
    // Get SS info
    StorageServerInfo* ss = (StorageServerInfo*)hashmap_get(ss_registry, info->ss_id);
    
    if (!ss || !ss->connected) {
        response->error_code = ERR_STORAGE_SERVER_DOWN;
        snprintf(response->data, BUFFER_SIZE, "Storage server unavailable");
        return;
    }
    
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "%s|%d", ss->ip, ss->client_port);
    
    log_message("NAME_SERVER", "INFO", "READ: %s redirecting %s to SS %s",
               msg->username, msg->filename, info->ss_id);
}

void handle_delete(Message* msg, Message* response) {
    FileInfo* info = (FileInfo*)hashmap_get(file_registry, msg->filename);
    
    if (!info) {
        response->error_code = ERR_FILE_NOT_FOUND;
        snprintf(response->data, BUFFER_SIZE, "File %s not found", msg->filename);
        return;
    }
    
    // Check ownership
    if (strcmp(info->owner, msg->username) != 0) {
        response->error_code = ERR_UNAUTHORIZED;
        snprintf(response->data, BUFFER_SIZE, "Only owner can delete file");
        return;
    }
    
    hashmap_remove(file_registry, msg->filename);
    lru_remove(file_cache, msg->filename);
    save_file_registry();
    
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "File %s deleted", msg->filename);
    
    log_message("NAME_SERVER", "INFO", "File deleted: %s by %s", msg->filename, msg->username);
}

void handle_list(Message* msg, Message* response) {
    (void)msg;  // Mark as intentionally unused
    char keys[1000][MAX_USERNAME];
    int count = 0;
    hashmap_get_keys(user_registry, (char(*)[MAX_FILENAME])keys, &count);
    
    response->data[0] = '\0';
    int pos = 0;
    
    for (int i = 0; i < count && pos < BUFFER_SIZE - MAX_USERNAME - 2; i++) {
        pos += snprintf(response->data + pos, BUFFER_SIZE - pos, "%s|", keys[i]);
    }
    
    response->error_code = SUCCESS;
    log_message("NAME_SERVER", "INFO", "LIST command: %d users listed", count);
}

void handle_lock_acquire(Message* msg, Message* response) {
    // Parse sentence index from data
    int sentence_index;
    sscanf(msg->data, "%d", &sentence_index);
    
    // Create lock key: "filename:sentence_index"
    char lock_key[MAX_FILENAME + 32];
    snprintf(lock_key, sizeof(lock_key), "%s:%d", msg->filename, sentence_index);
    
    pthread_mutex_lock(&registry_lock);
    
    // Check if file exists
    FileInfo* info = (FileInfo*)hashmap_get(file_registry, msg->filename);
    if (!info) {
        response->error_code = ERR_FILE_NOT_FOUND;
        snprintf(response->data, BUFFER_SIZE, "File not found");
        pthread_mutex_unlock(&registry_lock);
        return;
    }
    
    // Check if sentence is already locked
    SentenceLock* existing_lock = (SentenceLock*)hashmap_get(sentence_locks, lock_key);
    if (existing_lock) {
        // Check if locked by same user (allow re-entrant)
        if (strcmp(existing_lock->locked_by, msg->username) == 0) {
            response->error_code = SUCCESS;
            snprintf(response->data, BUFFER_SIZE, "Lock already held by you");
            pthread_mutex_unlock(&registry_lock);
            log_message("NAME_SERVER", "INFO", "Lock re-acquired: %s by %s", lock_key, msg->username);
            return;
        }
        
        // Locked by another user
        response->error_code = ERR_FILE_LOCKED;
        snprintf(response->data, BUFFER_SIZE, "Sentence locked by %s", existing_lock->locked_by);
        pthread_mutex_unlock(&registry_lock);
        log_message("NAME_SERVER", "INFO", "Lock denied: %s (held by %s, requested by %s)", 
                   lock_key, existing_lock->locked_by, msg->username);
        return;
    }
    
    // Create new lock
    SentenceLock* lock = (SentenceLock*)malloc(sizeof(SentenceLock));
    strncpy(lock->filename, msg->filename, MAX_FILENAME - 1);
    lock->sentence_index = sentence_index;
    strncpy(lock->locked_by, msg->username, MAX_USERNAME - 1);
    lock->lock_time = time(NULL);
    pthread_mutex_init(&lock->mutex, NULL);
    
    hashmap_put(sentence_locks, lock_key, lock);
    
    // Get SS info to return
    StorageServerInfo* ss = (StorageServerInfo*)hashmap_get(ss_registry, info->ss_id);
    if (!ss) {
        response->error_code = ERR_STORAGE_SERVER_DOWN;
        snprintf(response->data, BUFFER_SIZE, "Storage server unavailable");
        pthread_mutex_unlock(&registry_lock);
        return;
    }
    
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "%s|%d", ss->ip, ss->client_port);
    
    pthread_mutex_unlock(&registry_lock);
    
    log_message("NAME_SERVER", "INFO", "Lock acquired: %s by %s", lock_key, msg->username);
}

void handle_lock_release(Message* msg, Message* response) {
    // Parse sentence index from data
    int sentence_index;
    sscanf(msg->data, "%d", &sentence_index);
    
    // Create lock key
    char lock_key[MAX_FILENAME + 32];
    snprintf(lock_key, sizeof(lock_key), "%s:%d", msg->filename, sentence_index);
    
    pthread_mutex_lock(&registry_lock);
    
    SentenceLock* lock = (SentenceLock*)hashmap_get(sentence_locks, lock_key);
    
    if (!lock) {
        response->error_code = ERR_INVALID_PARAMETERS;
        snprintf(response->data, BUFFER_SIZE, "No lock exists");
        pthread_mutex_unlock(&registry_lock);
        return;
    }
    
    // Verify lock owner
    if (strcmp(lock->locked_by, msg->username) != 0) {
        response->error_code = ERR_UNAUTHORIZED;
        snprintf(response->data, BUFFER_SIZE, "Lock owned by %s", lock->locked_by);
        pthread_mutex_unlock(&registry_lock);
        return;
    }
    
    // Remove lock (hashmap_remove will free it)
    pthread_mutex_destroy(&lock->mutex);
    hashmap_remove(sentence_locks, lock_key);
    
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "Lock released");
    
    pthread_mutex_unlock(&registry_lock);
    
    log_message("NAME_SERVER", "INFO", "Lock released: %s by %s", lock_key, msg->username);
}

void handle_exec(Message* msg, Message* response) {
    pthread_mutex_lock(&registry_lock);
    
    FileInfo* file_info = (FileInfo*)hashmap_get(file_registry, msg->filename);
    
    if (!file_info) {
        response->error_code = ERR_FILE_NOT_FOUND;
        snprintf(response->data, BUFFER_SIZE, "File not found");
        pthread_mutex_unlock(&registry_lock);
        return;
    }
    
    // Get SS info
    StorageServerInfo* ss = (StorageServerInfo*)hashmap_get(ss_registry, file_info->ss_id);
    
    if (!ss) {
        response->error_code = ERR_INTERNAL;
        snprintf(response->data, BUFFER_SIZE, "Storage server not available");
        pthread_mutex_unlock(&registry_lock);
        return;
    }
    
    pthread_mutex_unlock(&registry_lock);
    
    // Connect to SS to fetch file
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (ss_socket < 0) {
        response->error_code = ERR_INTERNAL;
        snprintf(response->data, BUFFER_SIZE, "Failed to connect to storage server");
        return;
    }
    
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss->ip);
    ss_addr.sin_port = htons(ss->client_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
        response->error_code = ERR_INTERNAL;
        snprintf(response->data, BUFFER_SIZE, "Failed to connect to storage server");
        close(ss_socket);
        return;
    }
    
    // Request file from SS
    Message ss_msg;
    memset(&ss_msg, 0, sizeof(Message));
    ss_msg.command = CMD_READ;
    strncpy(ss_msg.username, msg->username, MAX_USERNAME - 1);
    strncpy(ss_msg.filename, msg->filename, MAX_FILENAME - 1);
    
    send_message(ss_socket, &ss_msg);
    
    Message ss_response;
    if (receive_message(ss_socket, &ss_response) < 0 || ss_response.error_code != SUCCESS) {
        response->error_code = ss_response.error_code;
        snprintf(response->data, BUFFER_SIZE, "Failed to read file");
        close(ss_socket);
        return;
    }
    
    close(ss_socket);
    
    // Save to temp file and execute
    char temp_path[MAX_PATH];
    snprintf(temp_path, MAX_PATH, "/tmp/exec_%s_%ld", msg->filename, time(NULL));
    
    FILE* fp = fopen(temp_path, "w");
    if (!fp) {
        response->error_code = ERR_INTERNAL;
        snprintf(response->data, BUFFER_SIZE, "Failed to create temp file");
        return;
    }
    
    fputs(ss_response.data, fp);
    fclose(fp);
    
    // Make executable
    chmod(temp_path, 0755);
    
    // Execute and capture output
    char exec_cmd[MAX_PATH + 50];
    snprintf(exec_cmd, sizeof(exec_cmd), "%s 2>&1", temp_path);
    
    FILE* exec_fp = popen(exec_cmd, "r");
    if (!exec_fp) {
        response->error_code = ERR_EXEC_FAILED;
        snprintf(response->data, BUFFER_SIZE, "Execution failed");
        unlink(temp_path);
        return;
    }
    
    // Read output
    char output[BUFFER_SIZE];
    int bytes = fread(output, 1, BUFFER_SIZE - 1, exec_fp);
    output[bytes] = '\0';
    
    int exec_status = pclose(exec_fp);
    unlink(temp_path);
    
    if (exec_status != 0) {
        response->error_code = ERR_EXEC_FAILED;
        snprintf(response->data, BUFFER_SIZE, "Execution failed (exit code %d):\n%s", 
                 exec_status, output);
    } else {
        response->error_code = SUCCESS;
        strncpy(response->data, output, BUFFER_SIZE - 1);
    }
    
    log_message("NAME_SERVER", "INFO", "EXEC: %s by %s (exit code %d)", 
                msg->filename, msg->username, exec_status);
}

// BONUS: Access request queue
HashMap* access_requests;  // "filename:username" -> AccessRequest*

void handle_request_access(Message* msg, Message* response) {
    // Parse: filename
    FileInfo* file = (FileInfo*)hashmap_get(file_registry, msg->filename);
    if (!file) {
        response->error_code = ERR_FILE_NOT_FOUND;
        strcpy(response->data, "File not found");
        return;
    }
    
    char request_key[MAX_FILENAME + MAX_USERNAME + 2];
    snprintf(request_key, sizeof(request_key), "%s:%s", msg->filename, msg->username);
    
    AccessRequest* req = (AccessRequest*)malloc(sizeof(AccessRequest));
    strncpy(req->filename, msg->filename, MAX_FILENAME - 1);
    strncpy(req->requester, msg->username, MAX_USERNAME - 1);
    strncpy(req->owner, file->owner, MAX_USERNAME - 1);
    req->request_time = time(NULL);
    req->pending = 1;
    
    hashmap_put(access_requests, request_key, req);
    
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "Access request sent to %s", file->owner);
    log_message("NAME_SERVER", "INFO", "AccessRequest: %s by %s for %s", 
                msg->filename, msg->username, file->owner);
}

void handle_view_requests(Message* msg, Message* response) {
    // View pending access requests for the current user (owner)
    char keys[1000][MAX_FILENAME];
    int count = 0;
    hashmap_get_keys(access_requests, keys, &count);
    
    char result[BUFFER_SIZE] = "";
    int found = 0;
    
    for (int i = 0; i < count; i++) {
        AccessRequest* req = (AccessRequest*)hashmap_get(access_requests, keys[i]);
        if (req && req->pending && strcmp(req->owner, msg->username) == 0) {
            if (found > 0) strcat(result, "\n");
            char line[256];
            snprintf(line, sizeof(line), "%s requested access to %s", 
                     req->requester, req->filename);
            strcat(result, line);
            found++;
        }
    }
    
    response->error_code = SUCCESS;
    if (found == 0) {
        strcpy(response->data, "No pending access requests");
    } else {
        strncpy(response->data, result, BUFFER_SIZE - 1);
    }
    log_message("NAME_SERVER", "INFO", "ViewRequests: %s (%d found)", msg->username, found);
}

void handle_approve_request(Message* msg, Message* response) {
    // Parse: filename|requester
    char filename[MAX_FILENAME], requester[MAX_USERNAME];
    if (sscanf(msg->data, "%[^|]|%s", filename, requester) != 2) {
        response->error_code = ERR_INVALID_PARAMETERS;
        strcpy(response->data, "Invalid parameters");
        return;
    }
    
    char request_key[MAX_FILENAME + MAX_USERNAME + 2];
    snprintf(request_key, sizeof(request_key), "%s:%s", filename, requester);
    
    AccessRequest* req = (AccessRequest*)hashmap_get(access_requests, request_key);
    if (!req || !req->pending) {
        response->error_code = ERR_FILE_NOT_FOUND;
        strcpy(response->data, "Request not found");
        return;
    }
    
    // Check ownership
    FileInfo* file = (FileInfo*)hashmap_get(file_registry, filename);
    if (!file || strcmp(file->owner, msg->username) != 0) {
        response->error_code = ERR_UNAUTHORIZED;
        strcpy(response->data, "Not file owner");
        return;
    }
    
    // Grant read access by forwarding to storage server
    FileInfo* cached_info = (FileInfo*)lru_get(file_cache, filename);
    if (!cached_info) {
        cached_info = file;
    }
    
    StorageServerInfo* ss = (StorageServerInfo*)hashmap_get(ss_registry, cached_info->ss_id);
    if (!ss || !ss->connected) {
        response->error_code = ERR_STORAGE_SERVER_DOWN;
        strcpy(response->data, "Storage server unavailable");
        return;
    }
    
    // Connect to SS and grant access
    int ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_addr.s_addr = inet_addr(ss->ip);
    ss_addr.sin_port = htons(ss->nm_port);
    
    if (connect(ss_socket, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) >= 0) {
        Message ss_msg;
        memset(&ss_msg, 0, sizeof(Message));
        ss_msg.msg_type = MSG_SS_COMMAND;
        ss_msg.command = CMD_ADDACCESS;
        strncpy(ss_msg.filename, filename, MAX_FILENAME - 1);
        strncpy(ss_msg.username, msg->username, MAX_USERNAME - 1);
        snprintf(ss_msg.data, BUFFER_SIZE, "-R|%s", requester);
        
        send_message(ss_socket, &ss_msg);
        
        Message ss_response;
        if (receive_message(ss_socket, &ss_response) >= 0) {
            req->pending = 0;
            response->error_code = SUCCESS;
            snprintf(response->data, BUFFER_SIZE, "Access granted to %s", requester);
            log_message("NAME_SERVER", "INFO", "AccessApproved: %s for %s by %s", 
                        filename, requester, msg->username);
        } else {
            response->error_code = ERR_INTERNAL;
            strcpy(response->data, "Failed to grant access");
        }
        close(ss_socket);
    } else {
        response->error_code = ERR_STORAGE_SERVER_DOWN;
        strcpy(response->data, "Cannot connect to storage server");
    }
}

void handle_deny_request(Message* msg, Message* response) {
    // Parse: filename|requester
    char filename[MAX_FILENAME], requester[MAX_USERNAME];
    if (sscanf(msg->data, "%[^|]|%s", filename, requester) != 2) {
        response->error_code = ERR_INVALID_PARAMETERS;
        strcpy(response->data, "Invalid parameters");
        return;
    }
    
    char request_key[MAX_FILENAME + MAX_USERNAME + 2];
    snprintf(request_key, sizeof(request_key), "%s:%s", filename, requester);
    
    AccessRequest* req = (AccessRequest*)hashmap_get(access_requests, request_key);
    if (!req || !req->pending) {
        response->error_code = ERR_FILE_NOT_FOUND;
        strcpy(response->data, "Request not found");
        return;
    }
    
    // Check ownership
    FileInfo* file = (FileInfo*)hashmap_get(file_registry, filename);
    if (!file || strcmp(file->owner, msg->username) != 0) {
        response->error_code = ERR_UNAUTHORIZED;
        strcpy(response->data, "Not file owner");
        return;
    }
    
    req->pending = 0;
    response->error_code = SUCCESS;
    snprintf(response->data, BUFFER_SIZE, "Access denied to %s", requester);
    log_message("NAME_SERVER", "INFO", "AccessDenied: %s for %s by %s", 
                filename, requester, msg->username);
}

void* handle_client(void* arg) {
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
        
        switch (msg.msg_type) {
            case MSG_REGISTER_SS:
                handle_register_ss(&msg, &response);
                break;
            case MSG_REGISTER_USER:
                handle_register_user(&msg, &response);
                break;
            case MSG_COMMAND:
                switch (msg.command) {
                    case CMD_VIEW:
                        handle_view(&msg, &response);
                        break;
                    case CMD_CREATE:
                        handle_create(&msg, &response);
                        break;
                    case CMD_READ:
                        handle_read(&msg, &response);
                        break;
                    case CMD_DELETE:
                        handle_delete(&msg, &response);
                        break;
                    case CMD_LIST:
                        handle_list(&msg, &response);
                        break;
                    case CMD_EXEC:
                        handle_exec(&msg, &response);
                        break;
                    case CMD_LOCK_ACQUIRE:
                        handle_lock_acquire(&msg, &response);
                        break;
                    case CMD_LOCK_RELEASE:
                        handle_lock_release(&msg, &response);
                        break;
                    case CMD_REQUESTACCESS:
                        handle_request_access(&msg, &response);
                        break;
                    case CMD_VIEWREQUESTS:
                        handle_view_requests(&msg, &response);
                        break;
                    case CMD_APPROVEREQUEST:
                        handle_approve_request(&msg, &response);
                        break;
                    case CMD_DENYREQUEST:
                        handle_deny_request(&msg, &response);
                        break;
                    default:
                        response.error_code = ERR_INVALID_COMMAND;
                        snprintf(response.data, BUFFER_SIZE, "Command not implemented");
                }
                break;
            default:
                response.error_code = ERR_INVALID_COMMAND;
        }
        
        send_message(client_socket, &response);
    }
    
    close(client_socket);
    return NULL;
}

// BONUS: Heartbeat and failure detection thread
void* heartbeat_monitor(void* arg) {
    (void)arg;
    
    while (running) {
        sleep(10);  // Check every 10 seconds
        
        char keys[1000][MAX_FILENAME];
        int ss_count = 0;
        hashmap_get_keys(ss_registry, keys, &ss_count);
        
        time_t now = time(NULL);
        
        for (int i = 0; i < ss_count; i++) {
            StorageServerInfo* ss = (StorageServerInfo*)hashmap_get(ss_registry, keys[i]);
            if (ss && ss->connected) {
                // Check if last heartbeat was more than 30 seconds ago
                if (difftime(now, ss->last_heartbeat) > 30) {
                    ss->connected = 0;
                    log_message("NAME_SERVER", "WARNING", "Storage Server %s marked as down", ss->ss_id);
                    
                    // If it has a replica, mark replica as primary for its files
                    if (ss->replica_ss_id[0]) {
                        log_message("NAME_SERVER", "INFO", "Failing over to replica: %s", ss->replica_ss_id);
                    }
                }
            }
        }
    }
    
    return NULL;
}


int main() {
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    init_name_server();
    load_file_registry();
    
    // BONUS: Start heartbeat monitor thread
    pthread_t heartbeat_thread;
    pthread_create(&heartbeat_thread, NULL, heartbeat_monitor, NULL);
    pthread_detach(heartbeat_thread);
    
    // Create socket
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
        log_message("NAME_SERVER", "ERROR", "Failed to create socket: %s", strerror(errno));
        return 1;
    }
    
    int opt = 1;
    setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(NM_PORT);
    
    if (bind(server_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        log_message("NAME_SERVER", "ERROR", "Failed to bind: %s", strerror(errno));
        close(server_socket);
        return 1;
    }
    
    if (listen(server_socket, MAX_CLIENTS) < 0) {
        log_message("NAME_SERVER", "ERROR", "Failed to listen: %s", strerror(errno));
        close(server_socket);
        return 1;
    }
    
    log_message("NAME_SERVER", "INFO", "Name Server listening on port %d", NM_PORT);
    printf("Name Server started on port %d\n", NM_PORT);
    
    while (running) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        int* client_socket = malloc(sizeof(int));
        *client_socket = accept(server_socket, (struct sockaddr*)&client_addr, &client_len);
        
        if (*client_socket < 0) {
            if (running) {
                log_message("NAME_SERVER", "ERROR", "Accept failed: %s", strerror(errno));
            }
            free(client_socket);
            continue;
        }
        
        log_message("NAME_SERVER", "INFO", "New client connected from %s:%d",
                   inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
        
        pthread_t thread;
        pthread_create(&thread, NULL, handle_client, client_socket);
        pthread_detach(thread);
    }
    
    cleanup();
    return 0;
}
