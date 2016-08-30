#ifndef _ENCRYPTED_DAG_H_
#define _ENCRYPTED_DAG_H_

#include "common.h"
#include "util.h"
#include <set>
#include <vector>

class Node;
class DAG;

enum TASK_ID {
  // Big data benchmark 1
  TID_BD1_FILTER = 0,
  TID_BD1_PERMUTE_ROUND1 = 100,
  TID_BD1_PERMUTE_ROUND2 = 200,
  TID_BD1_PERMUTE_ROUND3 = 300,
  TID_BD1_PERMUTE_ROUND4 = 400,

  // Big data benchmark 2 
  TID_BD2_AGG_SORT1_ROUND1 = 1000,
  TID_BD2_AGG_SORT1_ROUND2 = 1100,
  TID_BD2_AGG_SORT1_ROUND3 = 1200,
  TID_BD2_AGG_SORT1_ROUND4 = 1300,
  
  TID_BD2_GROUPBY_STEP1 = 1500,
  TID_BD2_GROUPBY_STEP2 = 1600,
  TID_BD2_GROUPBY_STEP3 = 1700,
  
  TID_BD2_AGG_SORT2_ROUND1 = 2400,
  TID_BD2_AGG_SORT2_ROUND2 = 2500,
  TID_BD2_AGG_SORT2_ROUND3 = 2600,
  TID_BD2_AGG_SORT2_ROUND4 = 2700,
  
  TID_BD2_FILTER = 3000,
  TID_BD2_PROJECT = 3100,

  // Big data benchmark 3
  TID_BD3_SORT_PREPROCESS = 10000,
  TID_BD3_SORT1_STEP1 = 10100,
  TID_BD3_SORT1_STEP2 = 10200,
  TID_BD3_SORT1_STEP3 = 10300,
  TID_BD3_SORT1_STEP4 = 10400,
  
  TID_BD3_JOIN_STEP1 = 11100,
  TID_BD3_JOIN_STEP2 = 11200,
  TID_BD3_JOIN_STEP3 = 11300,
  
  TID_BD3_FILTER1 = 12000,
  
  TID_BD3_PERMUTE_STEP1 = 12100,
  TID_BD3_PERMUTE_STEP2 = 12200,
  TID_BD3_PERMUTE_STEP3 = 12300,
  TID_BD3_PERMUTE_STEP4 = 12400,

  TID_BD3_PROJECT1 = 16000,
  
  TID_BD3_AGG_SORT1_STEP1 = 13000,
  TID_BD3_AGG_SORT1_STEP2 = 13100,
  TID_BD3_AGG_SORT1_STEP3 = 13200,
  TID_BD3_AGG_SORT1_STEP4 = 13300,
  
  TID_BD3_GROUPBY_STEP1 = 14000,
  TID_BD3_GROUPBY_STEP2 = 14100,
  TID_BD3_GROUPBY_STEP3 = 14200,
  
  TID_BD3_AGG_SORT2_STEP1 = 14300,
  TID_BD3_AGG_SORT2_STEP2 = 14400,
  TID_BD3_AGG_SORT2_STEP3 = 14500,
  TID_BD3_AGG_SORT2_STEP4 = 14600,

  TID_BD3_PROJECT2 = 17000,

  TID_BD3_SORT_STEP1 = 15000,
  TID_BD3_SORT_STEP2 = 15100,
  TID_BD3_SORT_STEP3 = 15200,
  TID_BD3_SORT_STEP4 = 15300,


  TID_TEST_SORT = 20000,
  TID_TEST_AGG = 20010,
};

enum DAG_ID {
  DID_BD1,
  DID_BD2,
  DID_BD3,

  DID_ENC_BD1,
  DID_ENC_BD2,
  DID_ENC_BD3,

  DID_ENC_BD1_SINGLE,
  DID_ENC_BD2_SINGLE,
  DID_ENC_BD3_SINGLE,

  TEST_VERIFY,
};

enum ROUND_ORDER {
  FIRST = -1,
  LAST = 1,
  OTHER = 0,
};

class Node {
 public:
  Node(uint32_t id, uint32_t num_parents, uint32_t num_children);
  ~Node();

  uint32_t get_id();

  Node **parents;
  uint32_t num_parents;

  Node **children;
  uint32_t num_children;

  uint32_t id;
};

uint32_t task_id_parser(int op_code, int index);

class DAG {
 public:
  DAG(uint32_t num_nodes, uint32_t DAG_id);
  ~DAG();

  uint32_t get_task_id(int op_code, int index);
  std::set<uint32_t> *get_task_id_parents(uint32_t task_id);
  std::set<uint32_t> *get_task_id_children(uint32_t task_id);

  Node **nodes;
  uint32_t num_nodes;
  uint32_t DAG_id;
};

uint32_t task_id_parser(DAG *dag, int op_code, int index);

// Note: following *_set_edges() functions assume that all nodes have been created already

// Sort has 4 rounds
uint32_t sort_set_edges(DAG *dag, uint32_t num_part, uint32_t offset, int round_order);

uint32_t filter_set_edges(DAG *dag, uint32_t num_part, uint32_t offset, int round_order);

uint32_t project_set_edges(DAG *dag, uint32_t num_part, uint32_t offset, int round_order);

uint32_t sort_preprocess_set_edges(DAG *dag, uint32_t num_part, uint32_t offset, int round_order);

// GROUP BY edges
uint32_t agg_set_edges(DAG *dag, uint32_t num_part, uint32_t offset, int round_order);
// JOIN edges
uint32_t join_set_edges(DAG *dag, uint32_t num_part, uint32_t offset, int round_order);

// Encryption mode SORT edges
uint32_t enc_sort_set_edges(DAG *dag,
			    uint32_t num_part,
			    uint32_t offset,
			    int round_order);

uint32_t enc_agg_set_edges(DAG *dag,
			   uint32_t num_part,
			   uint32_t offset,
			   int round_order);

uint32_t enc_join_set_edges(DAG * dag,
			    uint32_t num_part,
			    uint32_t offset,
			    int round_order);

uint32_t get_benchmark_op_code(uint32_t op_code);
class DAGGenerator {
 public:
  static DAG *genDAG(int benchmark_op_code, uint32_t num_part);
};

bool set_verify(std::set<uint32_t> *set1,
		std::set<uint32_t> *set2);

#endif /* _ENCRYPTED_DAG_H_ */
