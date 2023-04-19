#include <sys/types.h>

#include "buf.h"

#define BLKSZ (64 * 1024)
#define BT_MAX_VALUE_SIZE (32)
#define BT_MAX_KEY_SIZE (8)
#define BT_MAX_HDR_SIZE (64)
#define BT_MAX_PATH_SIZE (10)

#define BT_LEAF (0)
#define BT_INNER (1)

/* The number of keys is 
 * (BLKSZ - BT_MAX_HDR_SIZE - BT_MAX_VALUE_SIZE) / (BT_MAX_KEY_SIZE + BT_MAX_VALUE_SIZE)
 */
#define BT_MAX_KEYS (1636)
#define SPLIT_KEYS (818)

#define BT_ISLEAF(node) ((node)->n_type == BT_LEAF)
#define BT_ISINNER(node) ((node)->n_type == BT_INNER)
#define BT_VALSZ(node) ((node)->n_tree->tr_vs)

typedef struct btnodehdr {
  uint32_t hdr_len;
  uint8_t  hdr_type;
  uint8_t  hdr_flags;
} btnodehdr;

typedef btnodehdr *btnodehdr_t;

/* 64 Bytes Structure */
typedef struct child_cont {
  unsigned char  vdata[BT_MAX_VALUE_SIZE];
} ct;

typedef struct btdata {
  btnodehdr bt_hdr;
  uint64_t  bt_keys[BT_MAX_KEYS];

/* Make sure to add one child for inner nodes */
  ct        bt_children[BT_MAX_KEYS + 1];
} btdata;

typedef btdata *btdata_t;

struct btree;
typedef btree *btree_t;

typedef struct btnode {
  struct buf  *n_bp;
  btdata_t    n_data;
  btree_t     n_tree;
  diskptr_t   n_ptr;
#define n_id    n_bp->bp_lblkno
#define n_hdr   n_data->bt_hdr
#define n_keys  n_data->bt_keys
#define n_ch    n_data->bt_children
#define n_len   n_data->bt_hdr.hdr_len
#define n_flags n_data->bt_hdr.hdr_flag
#define n_type  n_data->bt_hdr.hdr_type
} btnode;

typedef btnode *btnode_t;

typedef struct btree {
  diskptr_t   tr_ptr;
  size_t      tr_vs;
} btree;


int btree_init(btree_t tree, diskptr_t ptr, size_t value_size);
int btree_insert(btree_t tree, uint64_t key, void *value);
int btree_bulkinsert(btree_t tree, uint64_t *keys, void **values, size_t len);

int btree_delete(btree_t tree, uint64_t key, void *value);

int btree_find(btree_t tree, uint64_t key, void *value);
int btree_greater_equal(btree_t tree, uint64_t key, void *value);
int btree_smaller_equal(btree_t tree, uint64_t key, void *value);

int btree_rangequery(btree_t tree, uint64_t key_low, 
    uint64_t key_max, void *results, size_t results_max);

 diskptr_t btree_checkpoint(btree_t tree);

