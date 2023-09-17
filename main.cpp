#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <vector>
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <string>
#include <map>
#include <iostream>
#include "zset.h"
#include <algorithm>
#include "list.h"


#define container_of(ptr, type, member) ({                  \
const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
(type *)( (char *)__mptr - offsetof(type, member) );}) // using memory address to get the parent pointer of the member

struct Conn;
//define structure for storage data structure
struct {
    HMap db;
    //map of client connections, keyed by fd
    std::vector<Conn*> fd2conn;
    //timers
    DList idle_list;
}g_data;

std::uint64_t get_usec(){
    timespec  tv = {0 , 0};
    clock_gettime(CLOCK_MONOTONIC , &tv);
    return uint64_t(tv.tv_sec) * 1000000 + tv.tv_nsec / 1000;
}


struct Entry{
    HNode node;
    std::string key;
    std::string val;
    uint32_t type = 0;
    ZSet *zset = NULL;
};

bool entry_equal(HNode *lhs , HNode *rhs){
    Entry *le = container_of(lhs , Entry , node);
    Entry *ri = container_of(rhs , Entry , node);
    return lhs->hcode == rhs->hcode && le->key == ri->key;
}

uint64_t str_hash(const uint8_t *data , size_t len){
    uint32_t h = 0x811C9DC5;
    for(size_t i = 0 ; i < len ; i++){
        h = (h + data[i]) * 0x01000193;
    }
    return h;
}


const size_t k_max_msg = 4096;

enum {
    STATE_REQ = 0,
    STATE_RES = 1,
    STATE_END = 2,  // mark the connection for deletion
};

enum{
    SER_NIL = 0,
    SER_ERR = 1,
    SER_STR = 2,
    SER_INT = 3,
    SER_DBL = 4,
    SER_ARR = 5,
    };

struct Conn {
    int fd = -1;
    uint32_t state = 0;     // either STATE_REQ or STATE_RES
    // buffer for reading
    size_t rbuf_size = 0;
    uint8_t rbuf[4 + k_max_msg];
    // buffer for writing
    size_t wbuf_size = 0;
    size_t wbuf_sent = 0;
    uint8_t wbuf[4 + k_max_msg];
    uint64_t idle_start = 0;
    DList idle_list;
};


static void msg(const char *msg) {
    fprintf(stderr, "%s\n", msg);
}

static void die(const char *msg) {
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, msg);
    abort();
}


//ses the given file descriptor to non-blocking mode
static void fd_set_nb(int fd) {
    errno = 0;
    //get the current flag
    int flags = fcntl(fd, F_GETFL, 0);
    if (errno) {
        die("fcntl error");
        return;
    }

    //convert flag to non-blocking mode
    flags |= O_NONBLOCK;

    errno = 0;
    (void)fcntl(fd, F_SETFL, flags);
    if (errno) {
        die("fcntl error");
    }
}
enum {
    ERR_UNKNOWN = 1,
    ERR_2BIG = 2,
};

const size_t k_max_args = 1024;


bool str2dbl(const std::string &s, double &out) {
    char *endp = NULL;
    out = strtod(s.c_str(), &endp);
    return endp == s.c_str() + s.size() && !out;
}
bool str2int(const std::string &s, int64_t &out) {
    char *endp = NULL;
    out = strtoll(s.c_str(), &endp, 10);
    return endp == s.c_str() + s.size();
}
//helper methods for data serialization
//serialized data for NULL responses
void out_nil(std::string &out){
    out.push_back((char)SER_NIL);
}


void out_update_arr(std::string &out, uint32_t n) {
    assert(out[0] == SER_ARR);
    memcpy(&out[1], &n, 4);
}
//serialized data for std::string responses
void out_str(std::string &out , const std::string &val){
    out.push_back((char)SER_STR);
    uint32_t len = (uint32_t)val.size();
    out.append((char *)&len , 4);
    out.append(val);
}

void out_str(std::string &out, const char *s, size_t size) {
    out.push_back(SER_STR);
    uint32_t len = (uint32_t)size;
    out.append((char *)&len, 4);
    out.append(s, len);
}

//serialized data for int responses
void out_int(std::string &out , int val){
    out.push_back((char)SER_INT);
    out.append((char *)&val , 8);
}

//serialized data for double responses
void out_dbl(std::string &out, double val) {
    out.push_back(SER_DBL);
    out.append((char *)&val, 8);
}


//serialized data for error responses
void out_err(std::string &out , int32_t code , const std::string &msg){
    out.push_back((char)SER_ERR);
    out.append((char *)&code , 4);
    uint32_t len = (uint32_t)msg.size();
    out.append((char *)&len , 4);
    out.append(msg);
}

//serialize for header arr: (note that body of array is not included)
void out_arr(std::string &out , uint32_t n){
    out.push_back((char)SER_ARR);
    out.append((char *)&n , 4);
}

//scan entire hashtable append each val to the out(*args) string
void h_scan(Htable *tab , void(*f)(HNode * , void*) , void *arg){
    if(tab->size == 0){
        return;
    }
    for(size_t i = 0 ; i < tab->mask + 1 ; i++){
        HNode *node = tab->tab[i];
        while(node){
            f(node , arg); //serialize current node in hashmap into the output string
            node = node->next;
        }
    }
}

void do_zadd(std::vector<std::string> &cmd, std::string &out) {
    double score = 0;
    if (!str2dbl(cmd[2], score)) {
        return out_err(out, 10, "expect fp number");
    }

    // look up or create the zset
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_equal);

    Entry *ent = NULL;
    if (!hnode) {
        ent = new Entry();
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        ent->type = 1;
        ent->zset = new ZSet();
        hm_insert(&g_data.db, &ent->node);
    } else {
        ent = container_of(hnode, Entry, node);
        if (ent->type != 1) {
            return out_err(out, 10, "expect zset");
        }
    }

    // add or update the tuple
    const std::string &name = cmd[3];
    bool added = zset_add(ent->zset, name.data(), name.size(), score);
    return out_int(out, (int64_t)added);
}

bool expect_zset(std::string &out, std::string &s, Entry **ent) {
    Entry key;
    key.key.swap(s);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_equal);
    if (!hnode) {
        out_nil(out);
        return false;
    }

    *ent = container_of(hnode, Entry, node);
    if ((*ent)->type != 1) {
        out_err(out, 0 , "expect zset");
        return false;
    }
    return true;
}

// zrem zset name
void do_zrem(std::vector<std::string> &cmd, std::string &out) {
    Entry *ent = NULL;
    if (!expect_zset(out, cmd[1], &ent)) {
        return;
    }

    const std::string &name = cmd[2];
    ZNode *znode = zset_pop(ent->zset, name.data(), name.size());
    if (znode) {
        znode_del(znode);
    }
    return out_int(out, znode ? 1 : 0);
}
void cb_scan(HNode *node , void *arg){
    std::string &out = *(std::string *)arg;
    out_str(out , container_of(node , Entry , node)->key);
}


enum {
    ERR_TYPE = 3,
    ERR_ARG = 4,
    };

ZNode* znode_new(const char *name , size_t len , double score){
    ZNode *node = (ZNode *)malloc(sizeof(ZNode) + len);
    avl_init(&node->tree);
    node->hmap.next = NULL;
    node->hmap.hcode = str_hash((uint8_t *)name , len);
    node->score = score;
    node->len = len;
    memcpy(&node->name[0] , name , len);
    return node;
}

void do_zscore(std::vector<std::string> &cmd, std::string &out) {
    Entry *ent = NULL;
    if (!expect_zset(out, cmd[1], &ent)) {
        return;
    }

    const std::string &name = cmd[2];
    ZNode *znode = zset_lookup(ent->zset, name.data(), name.size());
    return znode ? out_dbl(out, znode->score) : out_nil(out);
}

//helper for comparing to ZNode
bool zless(AVLNode *lhs , double score , const char*name , size_t len){
    ZNode *zl = container_of(lhs , ZNode ,tree);
    if(zl->score != score){
        return zl->score < score;
    }
    int ret = memcmp(zl->name , name , std::min(zl->len , len));
    if(ret != 0){
        return ret < 0;
    }
    return zl->len < len;
}

bool zless(AVLNode *lhs , AVLNode *rhs){
    ZNode *zr = container_of(rhs , ZNode , tree);
    return zless(lhs , zr->score , zr->name , zr->len);
}

void do_zquery(std::vector<std::string> &cmd, std::string &out) {
    // parse args
    double score = 0;
    if (!str2dbl(cmd[2], score)) {
        return out_err(out, ERR_ARG, "expect fp number");
    }
    const std::string &name = cmd[3];
    int64_t offset = 0;
    int64_t limit = 0;
    if (!str2int(cmd[4], offset)) {
        return out_err(out, ERR_ARG, "expect int");
    }
    if (!str2int(cmd[5], limit)) {
        return out_err(out, ERR_ARG, "expect int");
    }

    // get the zset
    Entry *ent = NULL;
    if (!expect_zset(out, cmd[1], &ent)) {
        if (out[0] == SER_NIL) {
            out.clear();
            out_arr(out, 0);
        }
        return;
    }

    // look up the tuple
    if (limit <= 0) {
        return out_arr(out, 0);
    }
    ZNode *znode = zset_query(
            ent->zset, score, name.data(), name.size(), offset
            );

    // output
    out_arr(out, 0);    // the array length will be updated later
    uint32_t n = 0;
    while (znode && (int64_t)n < limit) {
        out_str(out, znode->name, znode->len);
        out_dbl(out, znode->score);
        znode = container_of(avl_offset(&znode->tree, +1), ZNode, tree);
        n += 2;
    }
    return out_update_arr(out, n);
}



//function serialize a hashmap node and concatenate into the spring
void hashmap_node_append(HNode *node , void *arg){
    std::string &out = *(std::string *) arg;
    out_str(out , container_of(node , Entry , node)->key);
}

//parse data with arbitrary requests.
//parse one requests. each request follow format len | data
int32_t parse_req(const uint8_t *data , size_t len , std::vector<std::string>&out){
    //sanity check
    if(len < 4){
        return -1;
    }
    uint32_t n = 0;
    memcpy(&n , &data[0] , 4);
    if(n > k_max_args) return -1; // data too long

    size_t pos = 4;
    while(n--){
        if(pos + 4 > len){
            return -1;
        }
        uint32_t sz = 0;
        memcpy(&sz , &data[pos] , 4);
        if(pos + 4 + sz > len){
            return -1;
        }
        out.push_back(std::string ((char *)&data[pos + 4], sz));
        pos += 4 + sz;
    }
    if(pos != len){
        return -1;
    }
    return 0;
}

enum{
    RES_OK = 0,
    RES_ERR = 1,
    RES_NX = 2
};

std::map<std::string , std::string> mp;
//retrieve the data from the map

//get with implementation of with the in-house hashmap
void do_get(std::vector<std::string> &cmd , std::string &out){
    std::cout << "query for get \n";
    //recreate the entry based on the key, retrive from the hashmap
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data() , key.key.size());
    HNode *node = hm_lookup(&g_data.db , &key.node , &entry_equal);
    if(!node){
        return out_nil(out);
    }

    const std::string &val = container_of(node , Entry , node)->val;
    assert(val.size() <= k_max_msg); // stop if the string is too long to be sent via the networking module
    out_str(out , val);
    return;
}

void do_set(std::vector<std::string> &cmd , std::string &out){
    std::cout << "query for set \n";
    //two cases:
    //  - node/entry is already created
    //  - node/entry is yet created

    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((std::uint8_t *)key.key.data() , key.key.size());

    HNode *node = hm_lookup(&g_data.db , &key.node , &entry_equal);
    if(node){
        // if node is already created
        container_of(node , Entry , node)->val.swap(cmd[2]);
    }else{
        Entry *ent = new Entry();
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        ent->val.swap(cmd[2]);
        hm_insert(&g_data.db , &ent->node);
    }
    return out_nil(out);
}

void do_del(std::vector<std::string> &cmd , std::string &out){
    std::cout << "query for del \n";
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((std::uint8_t *)key.key.data() , key.key.size());
    HNode *node = hm_pop(&g_data.db , &key.node , &entry_equal);
    if(node){
        delete container_of(node , Entry , node); //RAII
    }
    return out_int(out , node ? 1 : 0);
}


bool cmd_is(const std::string &word , const char*cmd){
    return 0 == strcasecmp(word.c_str() , cmd);
}


void do_keys(std::vector<std::string> &cmd , std::string &out){
    (void)cmd;
    out_arr(out , (uint32_t)hm_size(&g_data.db));
    h_scan(&g_data.db.ht1 , &hashmap_node_append , &out);
    h_scan(&g_data.db.ht2 , &hashmap_node_append , &out);
}


void do_request(std::vector<std::string> &cmd , std::string &out){
    if (cmd.size() == 1 && cmd_is(cmd[0], "keys")) {
        do_keys(cmd, out);
    } else if (cmd.size() == 2 && cmd_is(cmd[0], "get")) {
        do_get(cmd, out);
    } else if (cmd.size() == 3 && cmd_is(cmd[0], "set")) {
        do_set(cmd, out);
    } else if (cmd.size() == 2 && cmd_is(cmd[0], "del")) {
        do_del(cmd, out);
    }else if (cmd.size() == 4 && cmd_is(cmd[0], "zadd")) {
        do_zadd(cmd, out);
    } else if (cmd.size() == 3 && cmd_is(cmd[0], "zrem")) {
        do_zrem(cmd, out);
    } else if (cmd.size() == 3 && cmd_is(cmd[0], "zscore")) {
        do_zscore(cmd, out);
    } else if (cmd.size() == 6 && cmd_is(cmd[0], "zquery")) {
        do_zquery(cmd, out);
    } else {
        // cmd is not recognized
        out_err(out, ERR_UNKNOWN , "Unknown cmd");
    }
}



//add the connection to the fd2Conn(mapping from fd to connection)
//key fd; val: pointer to connection
static void conn_put(std::vector<Conn *> &fd2Conn , struct Conn *conn){
    if(fd2Conn.size() <= (size_t)conn->fd){
        fd2Conn.resize((size_t)conn->fd + 1);
    }
    fd2Conn[conn->fd] = conn;
}

//given the listening fd, get the client fd and add it to the system
static int32_t accept_new_connection(std::vector<Conn *> &fd2conn , int fd){
    sockaddr_in client_addr = {};
    socklen_t socklen = sizeof(client_addr);
    int connfd = accept(fd , (sockaddr *)&client_addr , &socklen);
    if(connfd < 0){
        msg("accept() error");
        return -1;
    }
    fd_set_nb(connfd);

    Conn* conn = (struct Conn *)malloc(sizeof(struct Conn));
    if(!conn){
        close(connfd);
        return -1;
    }
    //fill in our paras for member element in the conn struct
    conn->fd = connfd;
    conn->state = STATE_REQ;
    conn->wbuf_size = 0;
    conn->rbuf_size = 0;
    conn->wbuf_sent = 0;
    conn->idle_start = get_usec();
    dlist_insert_before(&g_data.idle_list , &conn->idle_list);
    conn_put(g_data.fd2conn , conn);
    return 0;
}

static void state_req(Conn* conn);
static void state_res(Conn* conn);

//try parse one request from the buffer
static bool process_one_request(Conn *conn){
    if(conn->rbuf_size < 4){
        //not enough data to parse
        return false;
    }
    // read in the length prefix of in the read buffer
    uint32_t len = 0;
    memcpy(&len , &conn->rbuf[0] , 4);
    if(len > k_max_msg){
        msg("too long");
        conn->state = STATE_END;
        return false;
    }

    //if currently buffer does not contain all the message body
    if(conn->rbuf_size < 4  + len){
        return false;
    }

    //parse the request into vector of command
    std::vector<std::string> cmd;
    if(parse_req(&conn->rbuf[4] , len , cmd) != 0){
        msg("BAD REQUEST");
        conn->state = STATE_END;
        return false;
    }
    //process the request, generate the response
    std::string out;
    do_request(cmd , out);

    if (4 + out.size() > k_max_msg) {
        out.clear();
        out_err(out, ERR_2BIG , "response is too big");
    }
    uint32_t wlen = (uint32_t)out.size();

    memcpy(&conn->wbuf[0] , &wlen , 4);
    memcpy(&conn->wbuf[4] , out.data(), out.size()); //.data return the pointer to the first character of the character
    conn->wbuf_size = 4 + wlen;


    size_t remain = conn->rbuf_size - 4 - len;
    //here we remove the processed request from the read buffer
    if(remain){
        memmove(conn->rbuf , &conn->rbuf[4 + len] , remain);
    }
    conn->rbuf_size = remain;
    conn->state = STATE_RES;
    state_res(conn);
    return (conn->state == STATE_REQ);
}

//attempt to read data and process request
static bool process_buffer(Conn* conn){
    assert(conn->rbuf_size < sizeof(conn->rbuf));
    ssize_t rv = 0;
    do{
        size_t cap = sizeof(conn->rbuf) - conn->rbuf_size;
        rv = read(conn->fd , &conn->rbuf[conn->rbuf_size] , cap);
    }while(rv < 0 && errno == EINTR);

    if(rv < 0 && errno == EAGAIN){
        return false;
    }

    if (rv < 0) {
        msg("read() error");
        conn->state = STATE_END;
        return false;
    }
    if(rv == 0){
        if(conn->rbuf_size > 0){
            msg("unexpected EOF");
        }else{
            msg("EOF");
        }
        conn->state = STATE_END;
        return false;
    }

    conn->rbuf_size += (size_t)rv;
    assert(conn->rbuf_size <= sizeof(conn->rbuf));

    while(process_one_request(conn)){}
    return (conn->state == STATE_REQ);
}



static void state_req(Conn *conn){
    while (process_buffer(conn)){}
}

//try to sent response; return true if response is fully sent; false otherwise
static bool process_response(Conn *conn){
    size_t remain; ssize_t rv = 0;
    do{
        remain = conn->wbuf_size - conn->wbuf_sent;
        rv = write(conn->fd , &conn->wbuf[conn->wbuf_sent] , remain);
    }while(rv < 0 && errno == EINTR); //retry sending if interupted
    if (rv < 0 && errno == EAGAIN) {
        // got EAGAIN, stop.
        return false;
    }

    if(rv < 0){
        msg("write() error");
        conn->state = STATE_END;
        return false;
    }

    conn->wbuf_sent += (size_t)rv;
    assert(conn->wbuf_sent <= conn->wbuf_size);
    //if response fullly sent, return false
    if(conn->wbuf_sent == conn->wbuf_size){
        conn->state = STATE_REQ;
        conn->wbuf_sent = 0;
        conn->wbuf_size = 0;
        return false;
    }


    return true;
}

static void state_res(Conn* conn){
    while(process_response(conn)){}
}

const uint64_t k_idle_timeout_ms = 5 * 1000;

static void connection_handler(Conn *conn){
    //update date the idle variable when its being used
    conn->idle_start = get_usec();
    dlist_detach(&conn->idle_list);
    dlist_insert_before(&g_data.idle_list , &conn->idle_list);

    if(conn->state == STATE_REQ){
        state_req(conn);
    }else if(conn->state == STATE_RES){
        state_res(conn);
    }else{
        assert(0);
    }
}

void conn_done(Conn *conn){
    g_data.fd2conn[conn->fd] = NULL;
    (void)close(conn->fd);
    dlist_detach(&conn->idle_list);
    free(conn);
}

uint32_t next_timer_ms(){
    if(dlist_empty(&g_data.idle_list)){
        return 10000;
    }

    uint64_t now_us = get_usec();
    Conn *next = container_of(g_data.idle_list.next , Conn , idle_list);
    uint64_t next_us = get_usec();
    if(next_us < now_us){
        return 0;
    }
    return (uint32_t)((next_us - now_us) / 1000);
}

void process_timer(){
    uint64_t now_time = get_usec();
    while(!dlist_empty(&g_data.idle_list)){
        Conn *next = container_of(g_data.idle_list.next , Conn , idle_list);
        uint64_t next_time = next->idle_start + k_idle_timeout_ms * 1000;
        //check if time out

        if(next_time >= now_time + 1000){
            break;
        }
        printf("remove idle connection #%d \n" , next->fd);
        conn_done(next);
    }
}

int main(){
    //setup
    dlist_init(&g_data.idle_list);
    int listen_fd = socket(AF_INET , SOCK_STREAM , 0);
    if(listen_fd < 0){die("socket()");}
    int val = 1;
    setsockopt(listen_fd , SOL_SOCKET , SO_REUSEADDR , &val , sizeof(val));
    sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(0);
    //bind & listen for connection
    if(bind(listen_fd , (const sockaddr*)&addr , sizeof(addr))){die("bind()");}
    if(listen(listen_fd , SOMAXCONN)){die("listen()");}

    fd_set_nb(listen_fd);

    std::vector<struct pollfd> poll_args;
    while(true){
        poll_args.clear();
        pollfd pfd = {listen_fd , POLLIN , 0};
        poll_args.push_back(pfd);

        for(auto conn : g_data.fd2conn){
            if(!conn) continue;
            pollfd pfd = {};
            pfd.fd = conn->fd;
            pfd.events = (conn->state == STATE_REQ) ? POLLIN : POLLOUT;
            pfd.events = pfd.events | POLLERR;
            poll_args.push_back(pfd);
        }

        //poll for active fds
        int timeout_ms = (int)next_timer_ms();
        int rv = poll(poll_args.data() , (nfds_t)poll_args.size() , next_timer_ms());
        if(rv < 0) die("poll");

        for(size_t i = 1 ; i < poll_args.size() ; i++){
            if(poll_args[i].revents){
                Conn *conn = g_data.fd2conn[poll_args[i].fd];
                connection_handler(conn);
                if(conn->state == STATE_END){
                    conn_done(conn);
                }
            }
        }

        //clear off the idle ones
        process_timer();

        if(poll_args[0].revents){
            accept_new_connection(g_data.fd2conn , listen_fd);
        }
    }
    return 0;
}