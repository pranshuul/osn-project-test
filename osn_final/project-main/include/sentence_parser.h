#ifndef SENTENCE_PARSER_H
#define SENTENCE_PARSER_H

#include "common.h"

// Sentence parsing functions
int parse_sentences(const char* text, char sentences[][MAX_SENTENCE_LENGTH], int max_sentences);
int parse_words(const char* sentence, char words[][MAX_WORD_LENGTH], int max_words);
void rebuild_text(char sentences[][MAX_SENTENCE_LENGTH], int num_sentences, char* output, int max_len);
int insert_word(char* sentence, int word_index, const char* word, char* output, int max_len);
void get_text_stats(const char* text, int* word_count, int* char_count, int* sentence_count);

#endif // SENTENCE_PARSER_H
