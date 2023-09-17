//
// Created by Charles_Z on 2023-09-16.
//

#ifndef KEYFLARE_LIST_H
#define KEYFLARE_LIST_H

#endif //KEYFLARE_LIST_H

#include <stddef.h>

struct DList{
    DList *prev = NULL;
    DList *next = NULL;
};


void dlist_insert_before(DList *target , DList *dummy){
    DList *prev = target->prev;
    prev->next = dummy;
    dummy->prev = prev;
    dummy->next = target;
    target->prev = dummy;
}

void dlist_init(DList *node){
    node->prev = NULL;
    node->next = NULL;
}

bool dlist_empty(DList *node){
    return node->next == node;
}

void dlist_detach(DList *node){
    DList *prev = node->prev;
    DList *next = node->next;
    prev->next = next;
    next->prev = prev;
}
