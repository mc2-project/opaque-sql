#include "EncryptedDAG.h"

// this code is able to generate the correct DAG based on the op_code/benchmark


class Node {

  Node(uint32_t id, uint32_t num_parents, uint32_t num_children) {
	this->id = id;
	parents = malloc(sizeof(Node *) * num_parents);
	children = malloc(sizeof(Node *) * num_children);

	this->num_parents = num_parents;
	this->num_children = num_children;
  }

  ~Node() {
	// delete pointers to parents & children
	free(parents);
	free(children);
  }

  uint32_t get_id() {
	return id;
  }

  Node **parents;
  uint32_t num_parents;
  
  Node **children;
  uint32_t num_children;
  
  uint32_t id;
};


class DAG {

public:
  DAG(uint32_t num_nodes, uint32_t DAG_id) {
	this->num_nodes = num_nodes;
	nodes = malloc(sizeof(Node *) * num_nodes)
  }

  ~DAG() {
	for (uint32_t i = 0; i < num_nodes; i++) {
	  delete nodes[i];
	}
	free(nodes);
  }
  
  uint32_t get_task_id(int op_code, int index) {
	return task_id_parser(this, op_code, index);
  }

  Node **nodes;
  uint32_t num_nodes;
  uint32_t DAG_id;
};


// index defines the index of the partition
uint32_t task_id_parser(DAG *dag, int op_code, int index) {

  uint32_t tid = 0;

  switch(op_code) {
	/** BD1 **/
  case OP_BD1_FILTER:
	  tid = TID_BD1_FILTER;
	break;

  case OP_PERMUTE_BD1_SORT1:
	tid = TID_BD1_PERMUTE_ROUND1;
	break;

  case OP_PERMUTE_BD1_SORT2:
	tid = TID_BD1_PERMUTE_ROUND2;
	break;

  case OP_PERMUTE_BD1_SORT3:
	tid = TID_BD1_PERMUTE_ROUND3;
	break;

	/** BD2 **/
  case OP_BD2_FILTER_NOT_DUMMY:
	tid = TID_BD2_FILTER;
	break;

  case OP_BD2_PROJECT:
	tid = TID_BD2_PROJECT;
	break;

  case OP_BD2_GROUPBY_STEP1:
	tid = TID_BD2_GROUPBY_STEP1;
	break;

  case OP_BD2_GROUPBY_STEP2:
	tid = TID_BD2_GROUPBY_STEP2;
	break;

  case OP_BD2_GROUPBY_STEP3:
	tid = TID_BD2_GROUP_STEP3;
	break;

  case OP_BD2_SORT1_STEP1:
	tid = TID_BD2_AGG_SORT1_ROUND1;
	break;
	
  case OP_BD2_SORT1_STEP2:
	tid = TID_BD2_AGG_SORT1_ROUND2;
	break;
	
  case OP_BD2_SORT1_STEP3:
	tid = TID_BD2_AGG_SORT1_ROUND3;
	break;
	
  case OP_BD2_SORT2_STEP1:
	tid = TID_BD2_AGG_SORT2_ROUND1;	
	break;
  case OP_BD2_SORT2_STEP2:
	tid = TID_BD2_AGG_SORT2_ROUND2;	
	break;
  case OP_BD2_SORT2_STEP3:
	tid = TID_BD2_AGG_SORT2_ROUND3;	
	break;
	
	/** BD3 **/
  case OP_BD3_SORT_PREPROCESS:
	tid = TID_BD3_SORT_PREPROCESS;
	break;

  case OP_BD3_SORT1_STEP1:
	tid = TID_BD3_SORT1_STEP1;
	break;

  case OP_BD3_SORT1_STEP2:
	tid = TID_BD3_SORT1_STEP2;
	break;

  case OP_BD3_SORT1_STEP3:
	tid = TID_BD3_SORT1_STEP3;
	break;

  case OP_BD3_JOIN_STEP1:
	tid = TID_BD3_JOIN_STEP1;
	break;

  case OP_BD3_JOIN_STEP2:
	tid = TID_BD3_JOIN_STEP2;
	break;

  case OP_BD3_JOIN_STEP3:
	tid = TID_BD3_JOIN_STEP3;
	break;

  case OP_BD3_FILTER1:
	tid = TID_BD3_FILTER1;
	break;

  case OP_BD3_PERMUTE_STEP1:
	tid = TID_BD3_PERMUTE_STEP1;
	break;
	
  case OP_BD3_PERMUTE_STEP2:
	tid = TID_BD3_PERMUTE_STEP2;
	break;
	
  case OP_BD3_PERMUTE_STEP3:
	tid = TID_BD3_PERMUTE_STEP3;
	break;

  default:
	check(false, "task_id_parser: op_code not recognized");
	break;
  }

  return tid + index;
  
}


class DAGGenerator {

public:
  static DAG genDAG(int benchmark_op_code, uint32_t num_part) {

	DAG *dag = NULL;

	uint32_t num_nodes = 0;
	std::vector<Nodes *> nodes;
	
	switch(benchmark_op_code) {
	case BD1:
	  {	  
		// construct num_part of TID_BD1_FILTER
		num_nodes = num_part * 4;
		dag = new DAG(num_nodes, DID_BD1);

		// the first 3 rounds are permute
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i] = new Node(i + TID_BD1_PERMUTE_ROUND1, 1, num_part);
		}

		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part] = new Node(i + TID_BD1_PERMUTE_ROUND2, num_part, num_part);
		}

		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*2] = new Node(i + TID_BD1_PERMUTE_ROUND3, num_part, 1);
		}

		// filter
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*3] = new Node(i + TID_BD1_FILTER, 1, now);
		}

		
		// 1 that all nodes have been created, set the pointers

		// permute round 1
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i]->parents[0] = NULL;
		  for (uint32_t j = 0; j < num_part; j++) {
			dag->nodes[i]->children[j] = dag->nodes[j+num_part];
		  }
		}

		// permute round 2
		for (uint32_t i = 0; i < num_part; i++) {
		  for (uint32_t j = 0; j < num_part; j++) {
			dag->nodes[i+num_part]->parents[j] = dag->nodes[j];
		  }

		  for (uint32_t j = 0; j < num_part; j++) {
			dag->nodes[i]->children[j] = dag->nodes[j+num_part*@];
		  }
		}

		// permute round 3		
		for (uint32_t i = 0; i < num_part; i++) {
		  for (uint32_t j = 0; j < num_part; j++) {
			dag->nodes[i+num_part*2]->parents[j] = dag->nodes[j+num_part];
		  }
		  dag->nodes[i]->children[0] = dag->nodes[i + num_part*3];
		}

		// filter round
		for (uint32_t i = 0; i < num_part; i++) {
		  for (uint32_t j = 0; j < num_part; j++) {
			dag->nodes[i+num_part*2]->parents[j] = dag->nodes[j+num_part];
		  }
		  dag->nodes[i]->children[0] = dag->nodes[i + num_part*3];
		}
	  
	  }
	  break;

	case DID_BD2:
	  {
		num_nodes = num_part * (3 + 2 + 3 + 1 + 1) + 1;
		dag = new DAG(num_nodes, DID_BD2);
		uint32_t offset = 0;
		
		// AGG sort 1 (3 rounds)
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i] = new Node(i + TID_BD2_AGG_SORT1_ROUND1, 1, num_part);
		}
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part] = new Node(i + TID_BD2_AGG_SORT1_ROUND2, num_part, num_part);
		}
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*2] = new Node(i + TID_BD2_AGG_SORT1_ROUND2, num_part, 1);
		}

		// group by (3 rounds, 2nd round has only one task)
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*3] = new Node(i + TID_BD2_GROUPBY_STEP1, 1, 1);
		}
		dag->nodes[num_part*3+1] = new Node(i + TID_BD2_GROUPBY_STEP2, num_part, num_part);
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*3+1] = new Node(i + TID_BD2_GROUPBY_STEP1, 1, 1);
		}
		
		// AGG sort 2 (3 rounds)
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*4+1] = new Node(i + TID_BD2_AGG_SORT2_ROUND1, 1, num_part);
		}
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*5+1] = new Node(i + TID_BD2_AGG_SORT2_ROUND2, num_part, num_part);
		}
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*6+1] = new Node(i + TID_BD2_AGG_SORT2_ROUND3, num_part, 1);
		}

		// filter
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*7+1] = new Node(i + TID_BD2_FILTER, 1, 1);
		}

		// project
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*8+1] = new Node(i + TID_BD2_PROJECT, 1, 1);
		}

		// set parents & children
		// AGG sort 1 (3 rounds)
 		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i]->parents[0] = NULL;
		  for (uint32_t j = 0; j < num_part; j++) {
			dag->nodes[i]->children[j] = dag->nodes[j+num_part];
		  }
		}
		for (uint32_t i = 0; i < num_part; i++) {
		  for (uint32_t j = 0; j++) {
			dag->nodes[i+num_part]->parents[j] = dag->nodes[j];
			dag->nodes[i+num_part]->children[j] = dag->nodes[j+num_part*2];
		  }
		}
		for (uint32_t i = 0; i < num_part; i++) {
		  for (uint32_t j = 0; j++) {
			dag->nodes[i+num_part*2]->parents[j] = dag->nodes[j+num_part];
		  }
		  dag->nodes[i+num_part*2]->children[0] = dag->nodes[i+num_part*3];
		}

		// group by (3 rounds)
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*3]->parents[0] = dag->nodes[i+num_part*2];
		  dag->nodes[i+num_part*3]->children[0] = dag->nodes[j+num_part*3+1];
		}
		for (uint32_t j = 0; j < num_part; j++) {
		  dag->nodes[num_part*3+1]->parents[j] = dag->nodes[j+num_part*3];
		  dag->nodes[num_part*3+1]->children[j] = dag->nodes[j+num_part*3+1];
		}
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*3+1]->parents[0] = dag->nodes[num_part*3+1];
		  for (uint32_t j = 0; j < num_part; j++) {
			dag->nodes[i]->children[j] = dag->nodes[j+num_part*4+1];
		  }
		}

		offset = num_part * 4 + 1;
		
		// AGG sort 2
		offset += sort_set_edges(dag, num_part, offset, false);

		// filter
		offset += filter_set_edges(dag, num_part, offset, false);

		offset += project_set_edges(dag, num_part, offset, true);
		
	  }
	  break;

	case DID_BD3:
	  {
		num_nodes = num_part * (1 + 3 + 2 + 3 + 1 + 1 + 2 + 1 + 3) + 1 + 1;
		dag = new DAG(num_nodes, DID_BD2);
		
		// sort preprocess
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i] = new Node(i + TID_BD3_SORT_PREPROCESS, 1, 1);
		}

		// sort1 (3 rounds)
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*1] = new Node(i + TID_BD3_SORT1_STEP1, 1, num_part);
		}
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*2] = new Node(i + TID_BD3_SORT1_STEP2, num_part, num_part);
		}
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*3] = new Node(i + TID_BD3_SORT1_STEP3, num_part, 1);
		}

		// join (3 rounds)
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*4] = new Node(i + TID_BD3_JOIN_STEP1, 1, 1);
		}
		dag->nodes[i+num_part*4+1] = new Node(i + TID_BD3_JOIN_STEP2, num_part, num_part);
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*5+1] = new Node(i + TID_BD3_JOIN_STEP3, 1, 1);
		}

		// permute (3 rounds)
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*6+1] = new Node(i + TID_BD3_PERMUTE_STEP1, 1, num_part);
		}
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*7+1] = new Node(i + TID_BD3_PERMUTE_STEP2, num_part, num_part);
		}
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*8+1] = new Node(i + TID_BD3_PERMUTE_STEP3, num_part, 1);
		}
		
		// filter
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*9+1] = new Node(i + TID_BD3_FILTER1, 1, 1);
		}

		// project
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*10+1] = new Node(i + TID_BD3_PROJECT1, 1, 1);
		}

		// aggregate
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*11+1] = new Node(i + TID_BD3_GROUPBY_STEP1, 1, 1);
		}
		dag->nodes[i+num_part*11+2] = new Node(i + TID_BD3_GROUPBY_STEP2, num_part, num_part);
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*12+2] = new Node(i + TID_BD3_GROUPBY_STEP3, 1, 1);
		}	   

		// project
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*13+2] = new Node(i + TID_BD3_PROJECT2, 1, 1);
		}		
		
		// sort
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*14+2] = new Node(i + TID_BD3_SORT_STEP1, 1, num_part);
		}
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*15+2] = new Node(i + TID_BD3_SORT_STEP2, num_part, num_part);
		}
		for (uint32_t i = 0; i < num_part; i++) {
		  dag->nodes[i+num_part*16+2] = new Node(i + TID_BD3_SORT_STEP3, num_part, 1);
		}
		
	  }
	  break;

	default:
	  {
		check(false, "DAGGenerator::genDAG(): cannot generate DAG, op code not found");
	  }
	  break;
	}
  }

  
};


