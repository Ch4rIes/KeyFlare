//
// Created by Charles_Z on 2023-09-01.
//

#include <assert.h>
#include <stdlib.h>
#include "hashtable.h"

//chosen n have to be a power of two
static void h_init(Htable *htab , size_t n){
    assert((n > 0 && (n - 1) & n ) == 0);
    htab->tab = (HNode **) calloc(sizeof(HNode *) , n); //dynamically handle memory of the hashtable array
    htab->mask = n - 1;
    htab->size = 0;
}

//hashtable insertion: insert current node in front of the head of linked list
static void h_insert(Htable *htab , HNode *node){
    size_t pos = node->hcode & htab->mask; // calculate the index in the hashtable. similar to code % n
    HNode *next = htab->tab[pos];
    node->next = next;
    htab->tab[pos] = node;
    htab->size += 1;
}

//look up
//traverse linked list in given position
static HNode **h_lookup(Htable *htab , HNode *key , bool (*cmp)(HNode * , HNode *)){
    if(!htab->tab){
        return NULL;
    }

    size_t pos = key->hcode & htab->mask;
    HNode** head = &htab->tab[pos];
    while(*head && !cmp(key , *head)){
        head = &(*head)->next;
    }
    return head; // if key not contained, return NULL
}

static HNode *h_detach(Htable *htab , HNode **from){
    HNode *node = *from;
    *from = (*from)->next;
    htab->size--;
    return node;
}

const size_t k_resizing_work = 128;

static void hm_help_resizing(HMap *hmap) {
    if (hmap->ht2.tab == NULL) {
        return;
    }

    size_t nwork = 0;
    while (nwork < k_resizing_work && hmap->ht2.size > 0) {
        // scan for nodes from ht2 and move them to ht1
        HNode **from = &hmap->ht2.tab[hmap->resizing_pos];
        if (!*from) {
            hmap->resizing_pos++;
            continue;
        }

        h_insert(&hmap->ht1, h_detach(&hmap->ht2, from));
        nwork++;
    }

    if (hmap->ht2.size == 0) {
        // done
        free(hmap->ht2.tab);
        hmap->ht2 = Htable{};
    }
}

//increase the capacity's size by two
static void hm_start_resizing(HMap *hmap){
    assert(hmap->ht2.tab == NULL);
    hmap->ht2 = hmap->ht1;
    h_init(&hmap->ht1 , (hmap->ht1.mask + 1) * 2);
    hmap->resizing_pos = 0;
}

//look up given node from the hashmap using hashtable function
HNode *hm_lookup(HMap *hmap, HNode *key, bool (*cmp)(HNode *, HNode *)){
    hm_help_resizing(hmap);
    HNode **from = h_lookup(&hmap->ht1, key, cmp);
    if (!from) {
        from = h_lookup(&hmap->ht2, key, cmp);
    }
    return from ? *from : NULL;
}

const size_t k_max_load_factor = 8;

void hm_insert(HMap *hmap , HNode *node){
    if(!hmap->ht1.tab){
        h_init(&hmap->ht1 , 4);
    }
    h_insert(&hmap->ht1 , node);
    if(!hmap->ht2.tab){
        size_t load_factor = hmap->ht1.size / (hmap->ht1.mask + 1);
        if(load_factor >= k_max_load_factor){
            hm_start_resizing(hmap);
        }
    }
    hm_help_resizing(hmap);
}

HNode *hm_pop(HMap *hmap , HNode *key , bool (*cmp)(HNode * , HNode*)){
    hm_help_resizing(hmap);
    HNode **from = h_lookup(&hmap->ht1 , key , cmp);
    if(from){
        return h_detach(&hmap->ht1 , from);
    }
    from = h_lookup(&hmap->ht2 , key , cmp);
    if(from){
        return h_detach(&hmap->ht2 , from);
    }
    return NULL;
}

void hm_destroy(HMap *hmap) {
    assert(hmap->ht1.size + hmap->ht2.size == 0);
    free(hmap->ht1.tab);
    free(hmap->ht2.tab);
    *hmap = HMap{};
}

size_t hm_size(HMap  *hmap){
    return hmap->ht1.size + hmap->ht2.size;
}