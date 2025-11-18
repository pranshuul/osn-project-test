#include "../include/common.h"
#include "../include/sentence_parser.h"
#include <signal.h>
#include <fcntl.h>

#define SS_CLIENT_PORT 7000
#define SS_ID "SS1"

int client_server_socket = -1;
int running = 1;
pthread_mutex_t file_mutex;

void cleanup_ss() {
    running = 0;
    if (client_server_socket >= 0) {
        close(client_server_socket);
    }
    pthread_mutex_destroy(&file_mutex);
    log_message("STORAGE_SERVER", "INFO", "Storage Server shutdown complete");
}

void signal_handler_ss(int signum) {
    log_message("STORAGE_SERVER", "INFO", "Received signal %d, shutting down...", signum);
    cleanup_ss();
    exit(0);
}

void register_with_nm() {
    int nm_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (nm_socket < 0) {
        log_message("STORAGE_SERVER", "ERROR", "Failed to create socket for NM");
        return;
    }
    
    struct sockaddr_in nm_addr;
    memset(&nm_addr, 0, sizeof(nm_addr));
    nm_addr.sin_family = AF_INET;
    nm_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    nm_addr.sin_port = htons(NM_PORT);
    
    if (connect(nm_socket, (struct sockaddr*)&nm_addr, sizeof(nm_addr)) < 0) {
        log_message("STORAGE_SERVER", "ERROR", "Failed to connect to NM: %s", strerror(errno));
        close(nm_socket);
        return;
    }
    
    Message msg;
    memset(&msg, 0, sizeof(Message));
    msg.msg_type = MSG_REGISTER_SS;
    snprintf(msg.data, BUFFER_SIZE, "%s|127.0.0.1|%d|%d", SS_ID, 6000, SS_CLIENT_PORT);
    
    send_message(nm_socket, &msg);
    
    Message response;
    if (receive_message(nm_socket, &response) >= 0) {
        log_message("STORAGE_SERVER", "INFO", "Registered with NM: %s", response.data);
    }
    
    close(nm_socket);
}

int load_file_content(const char* filename, char* content, int max_len) {
    char filepath[MAX_PATH];
    snprintf(filepath, MAX_PATH, "data/files/%s", filename);
    
    FILE* fp = fopen(filepath, "r");
    if (!fp) {
        return -1;
    }
    
    int bytes_read = fread(content, 1, max_len - 1, fp);
    content[bytes_read] = '\0';
    fclose(fp);
    
    return bytes_read;
}

int save_file_content(const char* filename, const char* content) {
    char filepath[MAX_PATH];
    snprintf(filepath, MAX_PATH, "data/files/%s", filename);
    
    FILE* fp = fopen(filepath, "w");
    if (!fp) {
        log_message("STORAGE_SERVER", "ERROR", "Failed to create file %s", filepath);
        return -1;
    }
    
    fputs(content, fp);
    fclose(fp);
    
    log_message("STORAGE_SERVER", "INFO", "File saved: %s (%lu bytes)", filename, strlen(content));
    return 0;
}

// Load entire file content into dynamically allocated string
char* load_file(const char* filename) {
    char filepath[MAX_PATH];
    snprintf(filepath, MAX_PATH, "data/files/%s", filename);
    
    FILE* fp = fopen(filepath, "r");
    if (!fp) {
        return NULL;
    }
    
    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    
    char* content = (char*)malloc(size + 1);
    if (!content) {
        fclose(fp);
        return NULL;
    }
    
    size_t bytes = fread(content, 1, size, fp);
    content[bytes] = '\0';
    fclose(fp);
    
    return content;
}

// Save content to file
int save_file(const char* filename, const char* content) {
    char filepath[MAX_PATH];
    snprintf(filepath, MAX_PATH, "data/files/%s", filename);
    
    FILE* fp = fopen(filepath, "w");
    if (!fp) {
        return -1;
    }
    
    fputs(content, fp);
    fclose(fp);
    return 0;
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
