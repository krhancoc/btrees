#include <assert.h>

#include "buf.h"
#include "btree.h"

typedef struct bpath {
  uint64_t    p_len; 
  btnode      p_nodes[BT_MAX_PATH_SIZE];
  uint8_t     p_cur;
} bpath;

typedef bpath *bpath_t;

static int num_splits = 0;

int binary_search(uint64_t* arr, size_t size, uint64_t key) {
  size_t low = 0;
  size_t high = size;
  while (low < high) {
      size_t mid = low + (high - low) / 2;
      if (arr[mid] >= key) {
          high = mid;
      } else {
          low = mid + 1;
      }
  }
  if (low >= size) {
      return size;
  } else {
      return low;
  }
}

static void
btnode_print(btnode_t node) {
  printf("\nNode %lx %u\n", node->n_ptr.offset, node->n_len);
  for (int i = 0; i < node->n_len; i+=10) {
    printf("%d: ", i);
    for (int j = i; j < i + 10 && j < node->n_len; j++) {
      printf(" %lx |", node->n_keys[j]);
    }
    printf("\n");
  }
  if (BT_ISINNER(node)) {
    printf("\n====CHILDREN=====\n");
    for (int i = 0; i < (node->n_len + 1); i+=10) {
      printf("%d: ", i);
      for (int j = i; j < i + 10 && (j < (node->n_len + 1)); j++) {
        printf(" %lx |", ((diskptr_t *)(&node->n_ch[j]))->offset);
      }
      printf("\n");
    }
  }
  printf("\n==================\n");
}

static void
path_print(bpath_t path) {
  for (int i = 0; i < path->p_len; i++) {
    btnode_print(&path->p_nodes[i]);
  }
}

static void
btnode_init(btnode_t node, btree_t tree, diskptr_t ptr, int lk_flags)
{
  struct buf *bp = getblk(ptr.offset, ptr.size * PBLKSZ, lk_flags);
  node->n_bp = bp;
  node->n_data = (btdata_t)bp->bp_data;
  node->n_tree = tree;
  node->n_ptr = ptr;
}


/* Node is locked exclusively on create */
static void 
btnode_create(btnode_t node, btree_t tree, uint8_t type)
{
  diskptr_t ptr = allocate_blk(BLKSZ);
  btnode_init(node, tree, ptr, LK_EXCLUSIVE);
  node->n_type = type;
  node->n_len = 0;
}

static inline void
path_add(bpath_t path, btree_t tree, diskptr_t ptr, int lk_flags)
{
  btnode_init(&path->p_nodes[path->p_len], tree, ptr, lk_flags);
  path->p_cur = path->p_len;
  path->p_len += 1;
}

static inline btnode_t
path_getcur(bpath_t path)
{
  return &path->p_nodes[path->p_cur];
}

static inline void
path_unacquire(bpath_t path, int acquire_as)
{
  for(int i = 0; i < path->p_len; i++) {
    buf_unlock(path->p_nodes[i].n_bp, acquire_as);
  }
}

static inline void
path_backtrack(bpath_t path)
{
  if (path->p_cur > 0) {
    path->p_cur -= 1;
  }
}


static inline btnode_t
path_fixup_cur_parent(bpath_t path, btnode_t parent)
{
  int num_to_move = BT_MAX_PATH_SIZE - path->p_cur - 1;
  memmove(&path->p_nodes[path->p_cur + 1], &path->p_nodes[path->p_cur], num_to_move * sizeof(btnode));
  memcpy(&path->p_nodes[path->p_cur], parent, sizeof(btnode));
  path->p_cur += 1;
  path->p_len += 1;
  return path_getcur(path);
}

static inline void
path_copy(bpath_t dst, bpath_t src)
{
  memcpy(dst->p_nodes, src->p_nodes, src->p_len * sizeof(btnode));
}

static btnode_t
btnode_go_deeper(bpath_t path, uint64_t key, int acquire_as)
{
  int idx = 0;
  int cidx;

  btnode_t cur = path_getcur(path);

  idx = binary_search(cur->n_keys, cur->n_len, key);
  if (idx == cur->n_len) {
    cidx = cur->n_len;
  } else {
    uint64_t keyflag = cur->n_keys[idx];
    if (key > keyflag) {
      cidx = idx + 1;
    } else {
      cidx = idx;
    }
  }

  diskptr_t ptr = *(diskptr_t *)&cur->n_ch[cidx];
  path_add(path, cur->n_tree, ptr, acquire_as);

  return path_getcur(path);
}

/*
 * Finds the node which should hold param KEY.
 */
static btnode_t 
btnode_find_child(bpath_t path, uint64_t key, int acquire_as)
{
  btnode_t cur = path_getcur(path);
  while (BT_ISINNER(cur)) {
    cur = btnode_go_deeper(path, key, acquire_as);
  };

  return cur;
}

/* 
 * Within a node, find the key thats greater than or equal
 * to value in param KEY.
 * Function will overwrite key with any key that is found
 * so call should ensure to save the real key before calling
 * and check after
 */
static int
btnode_find_ge(btree_t tree, uint64_t *key, void *value, int acquire_as) 
{
  btnode_t node;
  int idx;
  bpath path;
  path.p_len = 0;
  path_add(&path, tree, tree->tr_ptr, acquire_as);

  node = btnode_find_child(&path, *key, acquire_as);

  idx = binary_search(node->n_keys, node->n_len, *key);
  /* Is there no key here */
  if (idx >= node->n_len) {
    path_unacquire(&path, acquire_as);
    return -1;
  }

  *key = node->n_keys[idx];
      // key is greater than all elements in the array
  memcpy(value, &node->n_ch[idx + 1], tree->tr_vs);

  path_unacquire(&path, acquire_as);

  return 0;
}

static inline btnode_t
path_parent(bpath_t path)
{
  if (path->p_cur == 0) {
    return NULL;
  }

  return &path->p_nodes[path->p_cur - 1];
}

static inline int
btnode_determine_index_in_parent(btnode_t parent, btnode_t node)
{
  assert(parent != NULL);
  uint64_t max_key = node->n_keys[node->n_len - 1];
  return binary_search(parent->n_keys, parent->n_len, max_key);
}

static void
btnode_inner_insert(btnode_t node, int idx, uint64_t key, diskptr_t value)
{
  assert(BT_ISINNER(node));
  if (node->n_len) {
    int num_to_move = node->n_len - idx + 1;
    memmove(&node->n_keys[idx + 1], &node->n_keys[idx], num_to_move * sizeof(key));
    memmove(&node->n_ch[idx + 2], &node->n_ch[idx + 1], num_to_move * BT_MAX_VALUE_SIZE);
  }


  node->n_keys[idx] = key;
  memcpy(&node->n_ch[idx + 1], &value, sizeof(value));
  node->n_len += 1;
}


static void
btnode_split(bpath_t path)
{
  int idx;
  btnode_t node = path_getcur(path);
  btnode_t pptr = path_parent(path);
  btnode parent;
  btnode right_child;

#ifdef DEBUG
  printf("[Split]\n");
#endif

  /* We are the root */
  if (pptr == NULL) {
    btnode_create(&parent, node->n_tree, BT_INNER);
    /* Set our current node to the child of our new parent */
    memcpy(&parent.n_ch[0], &node->n_ptr, sizeof(diskptr_t));

    node = path_fixup_cur_parent(path, &parent);

    /* Fixup root parent ptr in the tree */
    node->n_tree->tr_ptr = parent.n_ptr;

    idx = 0;
  } else {
    parent = *pptr;
    idx = btnode_determine_index_in_parent(&parent, node);
  }

  btnode_create(&right_child, node->n_tree, node->n_type);

  right_child.n_len = SPLIT_KEYS;

  if (BT_ISLEAF(node)) {
    node->n_len = SPLIT_KEYS;
  } else {
    node->n_len = SPLIT_KEYS - 1;
  }

  uint64_t split_key = node->n_keys[SPLIT_KEYS - 1];

  memcpy(&right_child.n_keys[0], &node->n_keys[SPLIT_KEYS], SPLIT_KEYS * sizeof(uint64_t));
  if (BT_ISLEAF(node))
    memcpy(&right_child.n_ch[0], &node->n_ch[SPLIT_KEYS], (SPLIT_KEYS + 1) * BT_MAX_VALUE_SIZE);
  else
    memcpy(&right_child.n_ch[0], &node->n_ch[SPLIT_KEYS], (SPLIT_KEYS + 1) * BT_MAX_VALUE_SIZE);

  /* Setting the pivot key here, with SPLIT_KEYS - 1, means elements to the right must be
   * strictly greater
   */
  btnode_inner_insert(&parent, idx, split_key, right_child.n_ptr);

  /* Unlock the right child */
  buf_unlock(right_child.n_bp, LK_EXCLUSIVE);

  if (parent.n_len == BT_MAX_KEYS) {
    path_backtrack(path);
    btnode_split(path);
  }
}


static void
btnode_leaf_insert(btnode_t node, int idx, uint64_t key, void *value)
{
  assert(BT_ISLEAF(node));
  int num_to_move = node->n_len - idx;
  if (num_to_move > 0) {
      memmove(&node->n_keys[idx+1], &node->n_keys[idx], num_to_move * sizeof(key));
  }

  if (num_to_move > 0) {
      memmove(&node->n_ch[idx+2], &node->n_ch[idx + 1], num_to_move * BT_MAX_VALUE_SIZE);
  }

  node->n_keys[idx] = key;
  memcpy(&node->n_ch[idx + 1], value, BT_VALSZ(node));
  node->n_len += 1;
}


static int
btnode_insert(bpath_t path, uint64_t key, void *value)
{
  int idx;

  btnode_find_child(path, key, LK_EXCLUSIVE);
  btnode_t node = path_getcur(path);
  idx = binary_search(node->n_keys, node->n_len, key);
  btnode_leaf_insert(node, idx, key, value);
  if (node->n_len == BT_MAX_KEYS) {
    btnode_split(path);
  }


  return 0;
}
static void
btnode_leaf_delete(btnode_t node, int idx, void *value)
{
  assert(BT_ISLEAF(node));
  int num_to_move = node->n_len - idx;

  if (value != NULL)
    memcpy(value, &node->n_ch[idx + 1], BT_VALSZ(node));

  if (num_to_move > 0) {
      memmove(&node->n_keys[idx], &node->n_keys[idx + 1], num_to_move * sizeof(uint64_t));
  }

  if (num_to_move > 0) {
      memmove(&node->n_ch[idx + 1], &node->n_ch[idx + 2], num_to_move * BT_MAX_VALUE_SIZE);
  }
  node->n_len -= 1;
}

static void
btnode_inner_collapse(bpath_t path)
{
  diskptr_t *ptr;

  btnode_t node = path_getcur(path);
  btnode_t parent = path_parent(path);

  /* We are the root, and if we've gotten to this point that means
   * We collapsed the last child into the parent so the parent need
   * to become a leaf again */
  if (parent == NULL) {
    /* Ensure we are a leaf now */
    node->n_type = BT_LEAF;
    return;
  }

  if (node->n_len == 0) {
    return;
  }

  /* Find our index */
  int idx;
  for (idx = 0; idx < parent->n_len + 1; idx++) {
    ptr = (diskptr_t *)&parent->n_ch[idx];
    if (memcmp(ptr, &node->n_ptr, sizeof(diskptr_t)) == 0)
      break;
  }

  assert(memcmp(ptr, &node->n_ptr, sizeof(diskptr_t)) == 0);
  int num_to_move = parent->n_len - idx;
  memmove(&parent->n_keys[idx], &parent->n_keys[idx + 1], (num_to_move) * sizeof(uint64_t));
  memmove(&parent->n_ch[idx], &parent->n_ch[idx + 1], num_to_move * BT_MAX_VALUE_SIZE);

  parent->n_len -= 1;
  if (parent->n_len == 0) {
    path_backtrack(path);
    btnode_inner_collapse(path);
  }
}

static int
btnode_delete(bpath_t path, uint64_t key, void *value)
{
  btnode_t node;
  int idx;

  node = btnode_find_child(path, key, LK_EXCLUSIVE);
  idx = binary_search(node->n_keys, node->n_len, key);
  if (node->n_keys[idx] != key) {
    return -1;
  }

  btnode_leaf_delete(node, idx, value);
    /* Grab the key so we can search for ourselves after words */

  /* Delete the node from the parent */
  if (node->n_len == 0) {
    btnode_inner_collapse(path);
  }

  return 0;
}

int 
btree_delete(btree_t tree, uint64_t key, void *value)
{
  int ret;
  bpath path;
  path.p_len = 0;
  path_add(&path, tree, tree->tr_ptr, LK_EXCLUSIVE);

  ret = btnode_delete(&path, key, value);

  path_unacquire(&path, LK_EXCLUSIVE);

  return ret;
}

int
btree_init(btree_t tree, diskptr_t ptr, size_t value_size)
{
  assert(value_size <= BT_MAX_VALUE_SIZE);

  tree->tr_ptr = ptr;
  tree->tr_vs = value_size;

  return 0;
}

int 
btree_insert(btree_t tree, uint64_t key, void *value)
{
  int ret;
  bpath path;
  path.p_len = 0;
#ifdef DEBUG
  printf("[Insert] %lu\n", key);
#endif


  path_add(&path, tree, tree->tr_ptr, LK_EXCLUSIVE);

  ret = btnode_insert(&path, key, value);

  path_unacquire(&path, LK_EXCLUSIVE);

  return (ret);
}

int 
btree_find(btree_t tree, uint64_t key, void *value)
{
  uint64_t possible_key = key;
  int error;
#ifdef DEBUG
  printf("[Find] %lu\n", key);
#endif

  error = btnode_find_ge(tree, &possible_key, value, LK_SHARED);
  if (error) {
    return (error);
  }

  if (possible_key != key) {
    return (-1);
  }

  return 0;
}
