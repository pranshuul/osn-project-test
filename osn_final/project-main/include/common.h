#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>

// Configuration
#define NM_PORT 5000
#define MAX_CLIENTS 100
#define BUFFER_SIZE 8192
#define MAX_FILENAME 256
#define MAX_USERNAME 64
#define MAX_PATH 512
#define MAX_SENTENCE_LENGTH 1024
#define MAX_SENTENCES 1000
#define MAX_WORD_LENGTH 128
#define MAX_WORDS 500
#define MAX_ACL_ENTRIES 50

// Error codes
#define SUCCESS 0
#define ERR_FILE_NOT_FOUND 1
#define ERR_UNAUTHORIZED 2
#define ERR_FILE_LOCKED 3
#define ERR_INVALID_INDEX 4
#define ERR_FILE_EXISTS 5
#define ERR_PERMISSION_DENIED 6
#define ERR_INVALID_COMMAND 7
#define ERR_STORAGE_SERVER_DOWN 8
#define ERR_INTERNAL 9
#define ERR_USER_NOT_FOUND 10
#define ERR_NO_STORAGE_SERVERS 11
#define ERR_INVALID_PARAMETERS 12
#define ERR_EXEC_FAILED 13

// Message types
#define MSG_REGISTER_SS 1
#define MSG_REGISTER_USER 2
#define MSG_COMMAND 3
#define MSG_RESPONSE 4
#define MSG_SS_COMMAND 5

// Command types
#define CMD_VIEW 1
#define CMD_READ 2
#define CMD_CREATE 3
#define CMD_WRITE 4
#define CMD_DELETE 5
#define CMD_INFO 6
#define CMD_LIST 7
#define CMD_ADDACCESS 8
#define CMD_REMACCESS 9
#define CMD_STREAM 10
#define CMD_UNDO 11
#define CMD_COPY 12
#define CMD_FILEINFO 13
#define CMD_EXEC 14
#define CMD_WRITE_COMMIT 15
#define CMD_LOCK_ACQUIRE 16
#define CMD_LOCK_RELEASE 17

// Bonus: Folder structure commands
#define CMD_CREATEFOLDER 18
#define CMD_MOVE 19
#define CMD_VIEWFOLDER 20

// Bonus: Checkpoint commands
#define CMD_CHECKPOINT 21
#define CMD_VIEWCHECKPOINT 22
#define CMD_REVERT 23
#define CMD_LISTCHECKPOINTS 24

// Bonus: Access request commands
#define CMD_REQUESTACCESS 25
#define CMD_VIEWREQUESTS 26
#define CMD_APPROVEREQUEST 27
#define CMD_DENYREQUEST 28

// Permissions
#define PERM_NONE 0
#define PERM_READ 1
#define PERM_WRITE 2

// Data structures
typedef struct {
    char filename[MAX_FILENAME];
    char owner[MAX_USERNAME];
    char ss_id[64];
    time_t created;
    time_t modified;
    time_t accessed;
    char last_accessed_by[MAX_USERNAME];
    int word_count;
    int char_count;
} FileInfo;

typedef struct {
    char username[MAX_USERNAME];
    int permission; // PERM_READ or PERM_WRITE
} ACLEntry;

typedef struct {
    char filename[MAX_FILENAME];
    int sentence_index;
    char locked_by[MAX_USERNAME];
    time_t lock_time;
    pthread_mutex_t mutex;
} SentenceLock;

typedef struct {
    char username[MAX_USERNAME];
    char ip[64];
    int port;
    time_t registered;
} UserInfo;

typedef struct {
    char ss_id[64];
    char ip[64];
    int nm_port;
    int client_port;
    int connected;
    time_t last_heartbeat;
    char files[1000][MAX_FILENAME];
    int file_count;
    char replica_ss_id[64];  // ID of replica SS (for fault tolerance)
} StorageServerInfo;

// Bonus: Access request structure
typedef struct {
    char filename[MAX_FILENAME];
    char requester[MAX_USERNAME];
    char owner[MAX_USERNAME];
    time_t request_time;
    int pending;  // 1 if pending, 0 if processed
} AccessRequest;

// Bonus: Checkpoint structure
typedef struct {
    char filename[MAX_FILENAME];
    char tag[64];
    char content[BUFFER_SIZE];
    time_t created;
} Checkpoint;

// Message structure for network communication
typedef struct {
    int msg_type;
    int command;
    int error_code;
    char username[MAX_USERNAME];
    char filename[MAX_FILENAME];
    char data[BUFFER_SIZE];
    int data_len;
} Message;

// Function prototypes
void log_message(const char* component, const char* level, const char* format, ...);
char* get_error_message(int error_code);
void send_message(int socket_fd, Message* msg);
int receive_message(int socket_fd, Message* msg);
char* get_timestamp();

#endif // COMMON_H
