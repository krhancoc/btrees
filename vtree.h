#include <sys/types.h>

#include "buf.h"
/*
 * Virtual Tree Interface
 */

/* Max value size for tree in bytes */
#define BT_MAX_VALUE_SIZE (32)

typedef struct kvp {
  uint64_t key;
  int error;
  unsigned char  data[BT_MAX_VALUE_SIZE];
} kvp;

typedef int (*vtree_init_t)(void *tree, diskptr_t key, size_t value_size);

/* Write ops */
typedef int (*vtree_insert_t)(void *tree, uint64_t key, void *value);
typedef int (*vtree_bulkinsert_t)(void *tree, kvp *keyvalues, size_t len);
typedef int (*vtree_delete_t)(void *tree, uint64_t key, void *value);

/* Query Ops */
typedef int (*vtree_find_t)(void *tree, uint64_t key, void *value);
typedef int (*vtree_ge_t)(void *tree, uint64_t *key, void *value);
typedef int (*vtree_rangequery_t)(void *tree, uint64_t keylow, 
    uint64_t keymax, kvp *results, size_t results_max);

typedef diskptr_t (*vtree_checkpoint_t)(void *tree);

struct vtreeops {
  vtree_init_t          vtree_init;

  vtree_insert_t        vtree_insert;
  vtree_bulkinsert_t    vtree_bulkinsert;
  vtree_delete_t        vtree_delete;

  vtree_find_t          vtree_find;
  vtree_ge_t            vtree_ge;
  vtree_rangequery_t    vtree_rangequery;

  vtree_checkpoint_t    vtree_checkpoint;
};

typedef struct vtree {
  void            *v_tree;
  struct vtreeops *v_ops;
} vtree;

#define VTREE_INIT(tree, ptr, keysize) \
  ((tree)->v_ops->vtree_init((tree)->v_tree, ptr, keysize))

#define VTREE_INSERT(tree, key, value) \
  ((tree)->v_ops->vtree_insert((tree)->v_tree, key, value))

#define VTREE_BULKINSERT(tree, kvp, len) \
  ((tree)->v_ops->vtree_bulkinsert((tree)->v_tree, kvp, len))

#define VTREE_DELETE(tree, key, value) \
  ((tree)->v_ops->vtree_delete((tree)->v_tree, key, value))

#define VTREE_FIND(tree, key, value) \
  ((tree)->v_ops->vtree_find((tree)->v_tree, key, value))

#define VTREE_GE(tree, key, value) \
  ((tree)->v_ops->vtree_ge((tree)->v_tree, key, value))

#define VTREE_RANGEQUERY(tree, keylow, keymax, results, results_max) \
  ((tree)->v_ops->vtree_rangequery((tree)->v_tree, \
    keylow, keymax, results, results_max))

