#include "../include/sentence_parser.h"
#include <ctype.h>

// Parse text into sentences (split on . ! ?)
int parse_sentences(const char* text, char sentences[][MAX_SENTENCE_LENGTH], int max_sentences) {
    if (!text || !sentences) return 0;
    
    int sentence_count = 0;
    int char_index = 0;
    
    for (int i = 0; text[i] != '\0' && sentence_count < max_sentences; i++) {
        sentences[sentence_count][char_index++] = text[i];
        
        // Check for sentence delimiters
        if (text[i] == '.' || text[i] == '!' || text[i] == '?') {
            sentences[sentence_count][char_index] = '\0';
            
            // Trim leading/trailing whitespace
            int start = 0;
            while (isspace(sentences[sentence_count][start])) start++;
            
            int end = strlen(sentences[sentence_count]) - 1;
            while (end > start && isspace(sentences[sentence_count][end])) end--;
            
            if (start > 0 || end < (int)strlen(sentences[sentence_count]) - 1) {
                memmove(sentences[sentence_count], sentences[sentence_count] + start, end - start + 1);
                sentences[sentence_count][end - start + 1] = '\0';
            }
            
            if (strlen(sentences[sentence_count]) > 0) {
                sentence_count++;
                char_index = 0;
            }
        }
        
        if (char_index >= MAX_SENTENCE_LENGTH - 1) {
            sentences[sentence_count][char_index] = '\0';
            sentence_count++;
            char_index = 0;
        }
    }
    
    // Add incomplete sentence if any
    if (char_index > 0 && sentence_count < max_sentences) {
        sentences[sentence_count][char_index] = '\0';
        
        // Trim whitespace
        int start = 0;
        while (isspace(sentences[sentence_count][start])) start++;
        
        if (start < char_index) {
            int end = char_index - 1;
            while (end > start && isspace(sentences[sentence_count][end])) end--;
            
            memmove(sentences[sentence_count], sentences[sentence_count] + start, end - start + 1);
            sentences[sentence_count][end - start + 1] = '\0';
            
            if (strlen(sentences[sentence_count]) > 0) {
                sentence_count++;
            }
        }
    }
    
    return sentence_count;
}

// Parse sentence into words (space-separated)
int parse_words(const char* sentence, char words[][MAX_WORD_LENGTH], int max_words) {
    if (!sentence || !words) return 0;
    
    int word_count = 0;
    int char_index = 0;
    int in_word = 0;
    
    for (int i = 0; sentence[i] != '\0' && word_count < max_words; i++) {
        if (isspace(sentence[i])) {
            if (in_word) {
                words[word_count][char_index] = '\0';
                word_count++;
                char_index = 0;
                in_word = 0;
            }
        } else {
            if (!in_word) {
                in_word = 1;
            }
            
            if (char_index < MAX_WORD_LENGTH - 1) {
                words[word_count][char_index++] = sentence[i];
            }
        }
    }
    
    // Add last word if any
    if (in_word && word_count < max_words) {
        words[word_count][char_index] = '\0';
        word_count++;
    }
    
    return word_count;
}

// Rebuild text from sentences
void rebuild_text(char sentences[][MAX_SENTENCE_LENGTH], int num_sentences, char* output, int max_len) {
    if (!sentences || !output || max_len <= 0) return;
    
    output[0] = '\0';
    int current_len = 0;
    
    for (int i = 0; i < num_sentences && current_len < max_len - 1; i++) {
        if (i > 0 && current_len < max_len - 2) {
            output[current_len++] = ' ';
            output[current_len] = '\0';
        }
        
        int sent_len = strlen(sentences[i]);
        int space_left = max_len - current_len - 1;
        
        if (sent_len > space_left) {
            sent_len = space_left;
        }
        
        strncat(output, sentences[i], sent_len);
        current_len += sent_len;
    }
}

// Insert word at specific position in sentence
int insert_word(char* sentence, int word_index, const char* word, char* output, int max_len) {
    if (!sentence || !word || !output) return -1;
    
    char words[MAX_WORDS][MAX_WORD_LENGTH];
    int word_count = parse_words(sentence, words, MAX_WORDS);
    
    if (word_index < 0 || word_index > word_count) {
        return ERR_INVALID_INDEX;
    }
    
    output[0] = '\0';
    int current_len = 0;
    
    // Add words before insertion point
    for (int i = 0; i < word_index && current_len < max_len - 1; i++) {
        if (i > 0 && current_len < max_len - 2) {
            output[current_len++] = ' ';
            output[current_len] = '\0';
        }
        
        strncat(output, words[i], max_len - current_len - 1);
        current_len = strlen(output);
    }
    
    // Add new word
    if (word_index > 0 && current_len < max_len - 2) {
        output[current_len++] = ' ';
        output[current_len] = '\0';
    }
    
    strncat(output, word, max_len - current_len - 1);
    current_len = strlen(output);
    
    // Add remaining words
    for (int i = word_index; i < word_count && current_len < max_len - 1; i++) {
        if (current_len > 0 && current_len < max_len - 2) {
            output[current_len++] = ' ';
            output[current_len] = '\0';
        }
        
        strncat(output, words[i], max_len - current_len - 1);
        current_len = strlen(output);
    }
    
    return SUCCESS;
}

// Get text statistics
void get_text_stats(const char* text, int* word_count, int* char_count, int* sentence_count) {
    if (!text) {
        if (word_count) *word_count = 0;
        if (char_count) *char_count = 0;
        if (sentence_count) *sentence_count = 0;
        return;
    }
    
    if (char_count) {
        *char_count = strlen(text);
    }
    
    if (sentence_count || word_count) {
        char sentences[MAX_SENTENCES][MAX_SENTENCE_LENGTH];
        int num_sentences = parse_sentences(text, sentences, MAX_SENTENCES);
        
        if (sentence_count) {
            *sentence_count = num_sentences;
        }
        
        if (word_count) {
            int total_words = 0;
            for (int i = 0; i < num_sentences; i++) {
                char words[MAX_WORDS][MAX_WORD_LENGTH];
                int num_words = parse_words(sentences[i], words, MAX_WORDS);
                total_words += num_words;
            }
            *word_count = total_words;
        }
    }
}
