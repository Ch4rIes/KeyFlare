//
// Created by Charles_Z on 2023-09-01.
//

#ifndef KEYFLARE_HASHTABLE_H
#define KEYFLARE_HASHTABLE_H

#endif //KEYFLARE_HASHTABLE_H

#include <stddef.h>
#include <stdint.h>

struct HNode{
    HNode *next = NULL;
    uint64_t hcode;
};

struct Htable{
    HNode ** tab = NULL; //pointer to an arry of HNode pointers
    size_t mask = 0;
    size_t size = 0;
};

struct HMap{
    Htable ht1 , ht2;
    size_t resizing_pos = 0;
};

HNode *hm_lookup(HMap *hmap , HNode *key , bool (*cmp)(HNode * , HNode *));
void hm_insert(HMap *hmap , HNode *node);
HNode *hm_pop(HMap *hmap , HNode *key , bool (*cmp)(HNode * , HNode*));
void hm_destroy(HMap *hmap);
size_t hm_size(HMap *hmap);