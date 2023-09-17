#ifndef KEYFLARE_AVL_H
#define KEYFLARE_AVL_H

#endif //KEYFLARE_AVL_H

#include <stddef.h>
#include <stdint.h>

struct AVLNode{
    uint32_t height = 0;
    uint32_t count = 0;
    AVLNode *left = NULL;
    AVLNode *right = NULL;
    AVLNode *parent = NULL;
};

inline void avl_init(AVLNode *node){
    node->height = 1;
    node->count = 0;
    node->left = NULL;
    node->right = NULL;
    node->parent = NULL;
}

AVLNode* avl_fix(AVLNode* node);
AVLNode* avl_del(AVLNode* node);
AVLNode* avl_offset(AVLNode* node , int64_t offset);
