#ifndef _ENCRYPTED_DAG_H_
#define _ENCRYPTED_DAG_H_

#include "common.h"
#include "util.h"
#include <set>

class Node;
class DAG;

enum TASK_ID {
  // Big data benchmark 1
  TID_BD1_FILTER = 0,
  TID_BD1_PERMUTE_ROUND1 = 100,
  TID_BD1_PERMUTE_ROUND2 = 200,
  TID_BD1_PERMUTE_ROUND3 = 300,

  // Big data benchmark 2 
  TID_BD2_AGG_SORT1_ROUND1 = 1000,
  TID_BD2_AGG_SORT1_ROUND2 = 1100,
  TID_BD2_AGG_SORT1_ROUND2 = 1200,
  
  TID_BD2_GROUPBY_STEP1 = 1300,
  TID_BD2_GROUPBY_STEP2 = 1400,
  TID_BD2_GROUPBY_STEP3 = 1500,
  
  TID_BD2_AGG_SORT2_ROUND1 = 2400,
  TID_BD2_AGG_SORT2_ROUND2 = 2500,
  TID_BD2_AGG_SORT2_ROUND3 = 2600,
  
  TID_BD2_FILTER = 1700,
  TID_BD2_PROJECT = 1800,

  // Big data benchmark 3
  TID_BD3_SORT_PREPROCESS = 10000,
  TID_BD3_SORT1_STEP1 = 10100,
  TID_BD3_SORT1_STEP2 = 10200,
  TID_BD3_SORT1_STEP3 = 10300,
  TID_BD3_JOIN_STEP1 = 11100,
  TID_BD3_JOIN_STEP2 = 11200,
  TID_BD3_JOIN_STEP3 = 11300,
  TID_BD3_FILTER1 = 12000,
  TID_BD3_PERMUTE_STEP1 = 12100,
  TID_BD3_PERMUTE_STEP2 = 12200,
  TID_BD3_PERMUTE_STEP3 - 12300,

  TID_BD3_PROJECT1 = 16000,
  
  TID_BD3_AGG_SORT2_STEP1 = 13000,
  TID_BD3_AGG_SORT2_STEP2 = 13100,
  TID_BD3_AGG_SORT2_STEP3 = 13200,
  TID_BD3_GROUPBY_STEP1 = 14000,
  TID_BD3_GROUPBY_STEP2 = 14100,
  TID_BD3_GROUPBY_STEP3 = 14200,
  TID_BD3_AGG_SORT2_STEP1 = 14300,
  TID_BD3_AGG_SORT2_STEP2 = 14400,
  TID_BD3_AGG_SORT2_STEP3 = 14500,

  TID_BD3_PROJECT2 = 17000,

  TID_BD3_SORT_STEP1 = 15000,
  TID_BD3_SORT_STEP2 = 15100,
  TID_BD3_SORT_STEP3 = 15200,


};

enum DAG_ID {
  DID_BD1,
  DID_BD2,
  DID_BD3,

  DID_ENC_BD1,
  DID_ENC_BD2,
  DID_ENC_BD3,

  DID_ENC_BD1_SINGLE,
};

uint32_t task_id_parser(DAG *dag, int op_code, int index);

// the following *_set_edges() functions assume that all nodes have been created already
uint32_t sort_set_edges(DAG *dag, uint32_t num_part, uint32_t offset, bool final_round) {
  for (uint32_t i = 0; i < num_part; i++) {
	for (uint32_t j = 0; j < num_part; j++) {
	  dag->nodes[i+offset]->children[j] = dag->nodes[j+num_part+offset];
	}
  }
  for (uint32_t i = 0; i < num_part; i++) {
	for (uint32_t j = 0; j++) {
	  dag->nodes[i+num_part+offset]->parents[j] = dag->nodes[j+offset];
	  dag->nodes[i+num_part+offset]->children[j] = dag->nodes[j+num_part*2+offset];
	}
  }
  for (uint32_t i = 0; i < num_part; i++) {
	for (uint32_t j = 0; j++) {
	  dag->nodes[i+num_part*2+offset]->parents[j] = dag->nodes[j+num_part+offset];
	}
	if (!final_round) {
	  dag->nodes[i+num_part*2+offset]->children[0] = dag->nodes[i+num_part*3+offset];
	}
  }

  if (!final_round) {
	// set parents for the next round
	for (uint32_t i = 0; i < num_part; i++) {
	  dag->nodes[i+num_part*3+offset]->parents[0] = dag->nodes[i+num_part*2+offset];
	}
  }

  return num_part * 3;
}

uint32_t filter_set_edges(DAG *dag, uint32_t num_part, uint32_t offset, bool last_round) {
  for (uint32_t i = 0; i < num_part; i++) {
	if (!final_round) {
	  dag->nodes[i+offset]->children[0] = dag->nodes[i+num_part+offset];
	}
  }		

  if (!final_round) {
	// set parents for the next round
	for (uint32_t i = 0; i < num_part; i++) {
	  dag->nodes[i+num_part+offset]->parents[0] = dag->nodes[i+offset];
	}
  }

  return num_part;
}

uint32_t project_set_edges(DAG *dag, uint32_t num_part, uint32_t offset, bool final_round) {
  for (uint32_t i = 0; i < num_part; i++) {
	if (!final_round) {
	  dag->nodes[i+offset]->children[0] = dag->nodes[i+num_part+offset];
	}
  }		

  if (!final_round) {
	// set parents for the next round
	for (uint32_t i = 0; i < num_part; i++) {
	  dag->nodes[i+num_part+offset]->parents[0] = dag->nodes[i+offset];
	}
  }

  return num_part;
}


uint32_t agg_set_edges(DAG *dag, uint32_t num_part, uint32_t offset, bool final_round) {
  for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset]->children[0] = dag->nodes[j+offset];
  }
  
  for (uint32_t j = 0; j < num_part; j++) {
	dag->nodes[offset+num_part+1]->parents[j] = dag->nodes[j+offset];
	dag->nodes[offset+num_part+1]->children[j] = dag->nodes[j+num_part+1+offset];
  }

  if (!final_round) {
	for (uint32_t i = 0; i < num_part; i++) {
	  dag->nodes[i+num_part+1+offset]->parents[0] = dag->nodes[num_part+1+offset];
	  for (uint32_t j = 0; j < num_part; j++) {
		dag->nodes[i]->children[j] = dag->nodes[j+num_part*4+1];
	  }
	}
  }
}

#endif /* _ENCRYPTED_DAG_H_ */
