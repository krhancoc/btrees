#include <assert.h>

#include "buf.h"
#include "btree.h"

typedef struct bpath {
  uint64_t    p_len; 
  btnode      p_nodes[BT_MAX_PATH_SIZE];
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
      // key is greater than all elements in the array
      return size;
  } else {
      // the first element greater than or equal to key was found
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
    for (int i = 0; i < node->n_len + 1; i++) {
      printf("| %lx |", ((diskptr_t *)(&node->n_ch[i]))->offset);
      if (i % 10 == 0) {
        printf("\n");
      }
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
btnode_init(btnode_t node, btree_t tree, diskptr_t ptr)
{
  struct buf *bp = getblk(ptr.offset, ptr.size * PBLKSZ);
  node->n_bp = bp;
  node->n_data = (btdata_t)bp->bp_data;
  node->n_tree = tree;
  node->n_ptr = ptr;
}


static void 
btnode_create(btnode_t node, btree_t tree, uint8_t type)
{
  diskptr_t ptr = allocate_blk(BLKSZ);
  btnode_init(node, tree, ptr);
  node->n_type = type;
  node->n_len = 0;
}

static inline void
path_add(bpath_t path, btree_t tree, diskptr_t ptr)
{
  btnode_init(&path->p_nodes[path->p_len], tree, ptr);
  path->p_len += 1;
}

static inline btnode_t
path_getcur(bpath_t path)
{
  return &path->p_nodes[path->p_len - 1];
}

static inline void
path_fixup_parent(bpath_t path, btnode_t parent)
{
  memmove(&path->p_nodes[path->p_len], path_getcur(path), sizeof(btnode));
  memcpy(&path->p_nodes[path->p_len - 1], parent, sizeof(btnode));
  path->p_len += 1;
}

static inline void
path_copy(bpath_t dst, bpath_t src)
{
  memcpy(dst->p_nodes, src->p_nodes, src->p_len * sizeof(btnode));
}

static btnode_t
btnode_go_deeper(bpath_t path, uint64_t key)
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

  printf("[Deeper] %d %d %lx\n", cur->n_len, cidx, key);
  diskptr_t ptr = *(diskptr_t *)&cur->n_ch[cidx];
  path_add(path, cur->n_tree, ptr);

  return path_getcur(path);
}

/*
 * Finds the node which should hold param KEY.
 */
static btnode_t 
btnode_find_child(bpath_t path, uint64_t key)
{
  btnode_t cur = path_getcur(path);
  while (BT_ISINNER(cur)) {
    cur = btnode_go_deeper(path, key);
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
btnode_find_ge(btree_t tree, uint64_t *key, void *value) 
{
  btnode_t node;
  int idx;
  bpath path;
  path.p_len = 0;

  path_add(&path, tree, tree->tr_ptr);

  node = btnode_find_child(&path, *key);

  idx = binary_search(node->n_keys, node->n_len, *key);
  /* Is there no key here */
  if (idx >= node->n_len) {
    printf("TRYING TO FIND %lx\n", *key);
    path_print(&path);
    return -1;
  }

  *key = node->n_keys[idx];
  memcpy(value, &node->n_ch[idx + 1], tree->tr_vs);

  return 0;
}

static inline btnode_t
btnode_parent(bpath_t path)
{
  if (path->p_len <= 1) {
    return NULL;
  }

  return &path->p_nodes[path->p_len - 2];
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
  btnode_t pptr = btnode_parent(path);
  btnode parent;
  btnode right_child;

  /* We are the root */
  if (pptr == NULL) {
    btnode_create(&parent, node->n_tree, BT_INNER);
    /* Set our current node to the child of our new parent */
    memcpy(&node->n_ch[0], &node->n_ptr, sizeof(diskptr_t));

    path_fixup_parent(path, &parent);
    node = path_getcur(path);

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

  memcpy(&right_child.n_keys[0], &node->n_keys[SPLIT_KEYS], SPLIT_KEYS * sizeof(uint64_t));
  memcpy(&right_child.n_ch[0], &node->n_ch[SPLIT_KEYS], (SPLIT_KEYS + 1) * BT_MAX_VALUE_SIZE);


  /* Setting the pivot key here, with SPLIT_KEYS - 1, means elements to the right must be
   * strictly greater
   */
  btnode_inner_insert(&parent, idx, node->n_keys[SPLIT_KEYS - 1], right_child.n_ptr);
  if (parent.n_len == BT_MAX_KEYS) {
    path->p_len -= 1;
    btnode_split(path);
  }
}


static void
btnode_leaf_insert(btnode_t node, int idx, uint64_t key, void *value)
{
  assert(BT_ISLEAF(node));
  printf("[Insert] %lx into %lx at %d\n", key, node->n_ptr.offset, idx);
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

  btnode_find_child(path, key);
  btnode_t node = path_getcur(path);
  idx = binary_search(node->n_keys, node->n_len, key);
  btnode_leaf_insert(node, idx, key, value);
  if (node->n_len == BT_MAX_KEYS) {
    btnode_split(path);
  }

  return 0;
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
  bpath path;
  path.p_len = 0;

  path_add(&path, tree, tree->tr_ptr);

  return btnode_insert(&path, key, value);
}

int 
btree_find(btree_t tree, uint64_t key, void *value)
{
  uint64_t possible_key = key;
  int error;

  error = btnode_find_ge(tree, &possible_key, value);
  if (error) {
    printf("Could not any key\n");
    return (error);
  }

  if (possible_key != key) {
    bpath path;
    path.p_len = 0;

    path_add(&path, tree, tree->tr_ptr);

    btnode_find_child(&path, key);
    path_print(&path);
    printf("Could not find exact key %lu\n", possible_key);
    return (-1);
  }

  return 0;
}
