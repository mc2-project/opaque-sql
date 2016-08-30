#include "EncryptedDAG.h"

// this code is able to generate the correct DAG based on the op_code/benchmark

// true if two are exactly equal
// false otherwise
bool set_verify(std::set<uint32_t> *set1,
		std::set<uint32_t> *set2) {
  
  std::set<uint32_t>::iterator it_begin = set1->begin();
  std::set<uint32_t>::iterator it_end = set1->end();
  std::set<uint32_t>::iterator it;

  printf("Set 1\n");
  for (it = it_begin; it != it_end; it++) {
    printf("%u\n", *it);
  }

  it_begin = set2->begin();
  it_end = set2->end();
  printf("Set 2\n");
  for (it = it_begin; it != it_end; it++) {
    printf("%u\n", *it);
  }
  
  it_begin = set1->begin();
  it_end = set1->end();

  for (it = it_begin; it != it_end; it++) {
    if (set2->find(*it) == set2->end()) {
      return false;
    }
  }

  it_begin = set2->begin();
  it_end = set2->end();

  for (it = it_begin; it != it_end; it++) {
    if (set1->find(*it) == set1->end()) {
      return false;
    }
  }


  return true;
}


Node::Node(uint32_t id, uint32_t num_parents, uint32_t num_children) {
  this->id = id;

  this->num_parents = num_parents;
  this->num_children = num_children;

  if (num_parents > 0) {
    parents = (Node **) malloc(sizeof(Node *) * num_parents);
  }

  if (num_children > 0) {
    children = (Node **) malloc(sizeof(Node *) * num_children);
  }

}

Node::~Node() {
  // delete pointers to parents & children
  free(parents);
  free(children);
}

uint32_t Node::get_id() {
  return id;
}


DAG::DAG(uint32_t num_nodes, uint32_t DAG_id) {
  this->num_nodes = num_nodes;
  nodes = (Node **) malloc(sizeof(Node *) * num_nodes);
  this->DAG_id = DAG_id;
}

DAG::~DAG() {
  for (uint32_t i = 0; i < num_nodes; i++) {
    delete nodes[i];
  }
  free(nodes);
}

uint32_t DAG::get_task_id(int op_code, int index) {
  return task_id_parser(op_code, index);
}

std::set<uint32_t> * DAG::get_task_id_parents(uint32_t task_id) {
  // find the correct task in nodes
  // return a list of parents' task IDs

  Node *node = NULL;

  for (uint32_t i = 0; i < num_nodes; i++) {
    printf("DAG::get_task_id_parents(): num_nodes is %u, nodes[i]->id is %u\n", num_nodes, nodes[i]->id);
    if (nodes[i]->id == task_id) {
      node = nodes[i];
    }
  }

  std::set<uint32_t> *parents = new std::set<uint32_t>();

  if (node == NULL) {
    return parents;
  }

  for (uint32_t i = 0; i < node->num_parents; i++) {
    parents->insert(node->parents[i]->id);
  }

  return parents;
}

std::set<uint32_t> * DAG::get_task_id_children(uint32_t task_id) {
  // find the correct task in nodes
  // return a list of parents' task IDs

  Node *node = NULL;

  for (uint32_t i = 0; i < num_nodes; i++) {
    if (nodes[i]->id == task_id) {
      node = nodes[i];
    }
  }

  std::set<uint32_t> *children_list = new std::set<uint32_t>();

  if (node == NULL) {
    return children_list;
  }

  for (uint32_t i = 0; i < node->num_children; i++) {
    children_list->insert(node->children[i]->id);
  }

  return children_list;
}

// index defines the index of the partition
uint32_t task_id_parser(int op_code, int index) {

  uint32_t tid = 0;

  printf("task_id_parser: tid is %u, op_code is %u\n", tid, op_code);

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
    tid = TID_BD2_GROUPBY_STEP3;
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

    /** TEST **/
  case OP_TEST_SORT:
    tid = TID_TEST_SORT;
    break;

  case OP_TEST_AGG:
    tid = TID_TEST_AGG;
    break;

  default:
    check(false, "task_id_parser: op_code not recognized");
    break;
  }
  
  return tid + index;  
}


DAG * DAGGenerator::genDAG(int benchmark_op_code, uint32_t num_part) {

  DAG *dag = NULL;

  uint32_t num_nodes = 0;
  std::vector<Node *> nodes;
  uint32_t offset = 0;
	
  switch(benchmark_op_code) {
  case BD1:
    {	  
      // construct num_part of TID_BD1_FILTER
      num_nodes = num_part * 4;
      dag = new DAG(num_nodes, DID_BD1);

      // the first 3 rounds are permute
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD1_PERMUTE_ROUND1, 1, num_part);
      }
      offset += num_part;

      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD1_PERMUTE_ROUND2, num_part, num_part);
      }
      offset += num_part;

      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD1_PERMUTE_ROUND3, num_part, 1);
      }
      offset += num_part;

      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD1_PERMUTE_ROUND4, num_part, 1);
      }
      offset += num_part;

      // filter
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD1_FILTER, 1, num_part);
      }
      offset += num_part;

      offset = 0;

      offset += sort_set_edges(dag, num_part, offset, OTHER);
      offset += filter_set_edges(dag, num_part, offset, LAST);	  
    }
    break;

  case DID_BD2:
    {
      num_nodes = num_part * (3 + 2 + 3 + 1 + 1) + 1;
      dag = new DAG(num_nodes, DID_BD2);
		
      // AGG sort 1 (3 rounds)
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD2_AGG_SORT1_ROUND1, 1, num_part);
      }
      offset += num_part;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD2_AGG_SORT1_ROUND2, num_part, num_part);
      }
      offset += num_part;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD2_AGG_SORT1_ROUND3, num_part, num_part);
      }
      offset += num_part;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD2_AGG_SORT1_ROUND4, num_part, 1);
      }
      offset += num_part;

      // group by (3 rounds, 2nd round has only one task)
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD2_GROUPBY_STEP1, 1, 1);
      }
      offset += num_part;
      dag->nodes[offset] = new Node(TID_BD2_GROUPBY_STEP2, num_part, num_part);
      offset++;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD2_GROUPBY_STEP1, 1, 1);
      }
      offset += num_part;
		
      // AGG sort 2 (4 rounds)
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD2_AGG_SORT2_ROUND1, 1, num_part);
      }
      offset += num_part;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD2_AGG_SORT2_ROUND2, num_part, num_part);
      }
      offset += num_part;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD2_AGG_SORT2_ROUND3, num_part, num_part);
      }
      offset += num_part;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD2_AGG_SORT2_ROUND4, num_part, 1);
      }
      offset += num_part;

      // filter
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD2_FILTER, 1, 1);
      }
      offset += num_part;
      // project
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD2_PROJECT, 1, 1);
      }
      offset += num_part;

      offset = 0;

      // set parents & children
      // AGG sort 1
      offset += sort_set_edges(dag, num_part, offset, FIRST);

      // group by (3 rounds)
      offset += agg_set_edges(dag, num_part, offset, OTHER);
		
      // AGG sort 2
      offset += sort_set_edges(dag, num_part, offset, OTHER);

      // filter
      offset += filter_set_edges(dag, num_part, offset, OTHER);

      offset += project_set_edges(dag, num_part, offset, LAST);
		
    }
    break;

  case DID_BD3:
    {
      num_nodes = num_part * (1 + 3 + 2 + 3 + 1 + 1 + 2 + 1 + 3) + 1 + 1;
      dag = new DAG(num_nodes, DID_BD2);
      offset = 0;
		
      // sort preprocess
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_SORT_PREPROCESS, 1, 1);
      }
      offset += num_part;

      // sort1 (4 rounds)
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_SORT1_STEP1, 1, num_part);
      }
      offset += num_part;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_SORT1_STEP2, num_part, num_part);
      }
      offset += num_part;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_SORT1_STEP3, num_part, num_part);
      }
      offset += num_part;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_SORT1_STEP4, num_part, 1);
      }
      offset += num_part;

      // join (3 rounds)
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_JOIN_STEP1, 1, 1);
      }
      offset += num_part;
      dag->nodes[offset] = new Node(TID_BD3_JOIN_STEP2, num_part, num_part);
      offset++;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_JOIN_STEP3, 1, 1);
      }
      offset += num_part;

      // permute (4 rounds)
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_PERMUTE_STEP1, 1, num_part);
      }
      offset += num_part;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+num_part] = new Node(i + TID_BD3_PERMUTE_STEP2, num_part, num_part);
      }
      offset += num_part;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+num_part] = new Node(i + TID_BD3_PERMUTE_STEP3, num_part, num_part);
      }
      offset += num_part;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+num_part] = new Node(i + TID_BD3_PERMUTE_STEP4, num_part, 1);
      }
      offset += num_part;
		
      // filter
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_FILTER1, 1, 1);
      }
      offset += num_part;

      // project
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_PROJECT1, 1, 1);
      }
      offset += num_part;

      // aggregate
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_GROUPBY_STEP1, 1, 1);
      }
      offset += num_part;
      dag->nodes[offset] = new Node(TID_BD3_GROUPBY_STEP2, num_part, num_part);
      offset += 1;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_GROUPBY_STEP3, 1, 1);
      }
      offset += num_part;

      // project
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_PROJECT2, 1, 1);
      }
      offset += num_part;
		
      // sort
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_SORT_STEP1, 1, num_part);
      }
      offset += num_part;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_SORT_STEP2, num_part, num_part);
      }
      offset += num_part;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_SORT_STEP3, num_part, num_part);
      }
      offset += num_part;
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_SORT_STEP4, num_part, 1);
      }
      offset += num_part;

      // define all parents and children
      offset = 0;
      offset += sort_preprocess_set_edges(dag, num_part, offset, FIRST);
      offset += sort_set_edges(dag, num_part, offset, OTHER);
      offset += join_set_edges(dag, num_part, offset, OTHER);
      offset += sort_set_edges(dag, num_part, offset, OTHER);
      offset += filter_set_edges(dag, num_part, offset, OTHER);
      offset += project_set_edges(dag, num_part, offset, OTHER);
      offset += agg_set_edges(dag, num_part, offset, OTHER);
      offset += project_set_edges(dag, num_part, offset, OTHER);
      offset += sort_set_edges(dag, num_part, offset, OTHER);
    }
    break;

    // single machine encrypted version
  case DID_ENC_BD1_SINGLE:
    {
      // there is only one round -- only a filter is needed
      num_nodes = 1;
      dag = new DAG(num_nodes, DID_ENC_BD1_SINGLE);

      // filter
      dag->nodes[0] = new Node(TID_BD1_FILTER, 0, 0);
    }
    break;

  case DID_ENC_BD2_SINGLE:
    {
      // sort, aggregate, project
      num_nodes = 3;
      dag = new DAG(num_nodes, DID_ENC_BD2_SINGLE);

      // sort
      dag->nodes[0] = new Node(TID_BD2_AGG_SORT1_ROUND1, 0, 1);
      // group by
      dag->nodes[1] = new Node(TID_BD2_GROUPBY_STEP1, 1, 1);
      // project
      dag->nodes[2] = new Node(TID_BD2_PROJECT, 1, 0);

      dag->nodes[0]->children[0] = dag->nodes[1];
      dag->nodes[1]->parents[0] = dag->nodes[0];
      dag->nodes[1]->children[0] = dag->nodes[2];
      dag->nodes[2]->parents[0] = dag->nodes[1];
    }
    break;

  case DID_ENC_BD3_SINGLE:
    {
      // sort, join, sort, agg, project, sort
      num_nodes = 6;
      dag = new DAG(num_nodes, DID_ENC_BD3_SINGLE);

      // sort
      dag->nodes[0] = new Node(TID_BD3_SORT1_STEP1, 0, 1);
      // join
      dag->nodes[1] = new Node(TID_BD3_JOIN_STEP1, 1, 1);
      // sort
      dag->nodes[2] = new Node(TID_BD3_AGG_SORT2_STEP1, 1, 1);
      // agg
      dag->nodes[3] = new Node(TID_BD3_GROUPBY_STEP1, 1, 1);
      // project
      dag->nodes[4] = new Node(TID_BD3_PROJECT2, 1, 1);
      // sort
      dag->nodes[5] = new Node(TID_BD3_SORT_STEP1, 1, 0);

      dag->nodes[0]->children[0] = dag->nodes[1];
		
      dag->nodes[1]->parents[0] = dag->nodes[0];
      dag->nodes[1]->children[0] = dag->nodes[2];
		
      dag->nodes[2]->parents[0] = dag->nodes[1];
      dag->nodes[2]->children[0] = dag->nodes[3];
		
      dag->nodes[3]->parents[0] = dag->nodes[2];
      dag->nodes[3]->children[0] = dag->nodes[4];
		
      dag->nodes[4]->parents[0] = dag->nodes[3];
      dag->nodes[4]->children[0] = dag->nodes[5];
		
      dag->nodes[5]->parents[0] = dag->nodes[4];
    }
    break;

  case DID_ENC_BD1:
    {
      // there is only one round -- only a filter is needed
      num_nodes = num_part;
      dag = new DAG(num_nodes, DID_ENC_BD1);

      // filter
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i] = new Node(i + TID_BD1_FILTER, 0, 0);
      }
    }
    break;

  case DID_ENC_BD2:
    {
      // sort, aggregate, project
      num_nodes = num_part * 5 + 1;
      dag = new DAG(num_nodes, DID_ENC_BD1);

      offset = 0;

      // create the nodes
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i] = new Node(i + TID_BD2_AGG_SORT1_ROUND1, 1, 1);
      }
      offset += num_part;

      dag->nodes[offset] = new Node(TID_BD2_AGG_SORT1_ROUND2, num_part, num_part);
      offset += 1;

      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD2_AGG_SORT1_ROUND3, 1, num_part);
      }
      offset += num_part;

      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD2_AGG_SORT1_ROUND4, num_part, 1);
      }
      offset += num_part;

      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD2_GROUPBY_STEP1, 1, 1);
      }
      offset += num_part;

      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD2_PROJECT, 1, 1);
      }
      offset += num_part;

      // sort
      offset += enc_sort_set_edges(dag, num_part, offset, FIRST);

      // agg
      offset += enc_agg_set_edges(dag, num_part, offset, OTHER);

      // project
      offset += project_set_edges(dag, num_part, offset, LAST);

    }
    break;

  case DID_ENC_BD3:
    {
      // sort, join, sort, agg, project, sort
      num_nodes = 0;
      dag = new DAG(num_nodes, DID_ENC_BD1);

      offset = 0;

      // create the sort, join, sort, agg, project nodes
      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i] = new Node(i + TID_BD3_SORT1_STEP1, 1, 1);
      }
      offset += num_part;

      dag->nodes[offset] = new Node(TID_BD3_SORT1_STEP2, num_part, num_part);
      offset += 1;

      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_SORT1_STEP3, 1, num_part);
      }
      offset += num_part;

      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_SORT1_STEP4, num_part, 1);
      }
      offset += num_part;

      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_JOIN_STEP1, 1, 1);
      }
      offset += num_part;

      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_AGG_SORT2_STEP1, 1, 1);
      }
      offset += num_part;

      dag->nodes[offset] = new Node(TID_BD3_AGG_SORT2_STEP2, num_part, num_part);
      offset += 1;

      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_AGG_SORT2_STEP3, 1, num_part);
      }
      offset += num_part;

      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_AGG_SORT2_STEP4, num_part, 1);
      }
      offset += num_part;

      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_GROUPBY_STEP1, 1, 1);
      }
      offset += num_part;

      for (uint32_t i = 0; i < num_part; i++) {
	dag->nodes[i+offset] = new Node(i + TID_BD3_PROJECT2, 1, 1);
      }
      offset += num_part;

      offset = 0;

      // sort
      offset += enc_sort_set_edges(dag, num_part, offset, FIRST);

      // join
      offset += enc_join_set_edges(dag, num_part, offset, OTHER);

      // sort
      offset += enc_sort_set_edges(dag, num_part, offset, OTHER);

      // agg
      offset += enc_agg_set_edges(dag, num_part, offset, OTHER);

      // project
      offset += project_set_edges(dag, num_part, offset, LAST);
    }
    break;

  case TEST_VERIFY:
    { 
      // sort, aggregate
      num_nodes = 2;
      dag = new DAG(num_nodes, DID_ENC_BD2_SINGLE);

      // sort
      dag->nodes[0] = new Node(TID_TEST_SORT, 0, 1);
      // group by
      dag->nodes[1] = new Node(TID_TEST_AGG, 1, 0);

      dag->nodes[0]->children[0] = dag->nodes[1];
      dag->nodes[1]->parents[0] = dag->nodes[0];
    }
    break;
	  
  default:
    {
      check(false, "DAGGenerator::genDAG(): cannot generate DAG, op code not found");
    }
    break;
  }

  return dag;
}


uint32_t sort_set_edges(DAG *dag, uint32_t num_part, uint32_t offset, int round_order) {

  // first, 4 rounds of setting parents and children

  for (uint32_t i = 0; i < num_part; i++) {
    for (uint32_t j = 0; j < num_part; j++) {
      dag->nodes[i+offset]->children[j] = dag->nodes[j+num_part+offset];
    }
  }
  
  for (uint32_t i = 0; i < num_part; i++) {
    for (uint32_t j = 0; j < num_part; j++) {
      dag->nodes[i+num_part+offset]->parents[j] = dag->nodes[j+offset];
      dag->nodes[i+num_part+offset]->children[j] = dag->nodes[j+num_part*2+offset];
    }
  }
  
  for (uint32_t i = 0; i < num_part; i++) {
    for (uint32_t j = 0; j < num_part; j++) {
      dag->nodes[i+num_part*2+offset]->parents[j] = dag->nodes[j+num_part+offset];
      dag->nodes[i+num_part*2+offset]->children[j] = dag->nodes[j+num_part*3+offset];
    }
  }

  for (uint32_t i = 0; i < num_part; i++) {
    for (uint32_t j = 0; j < num_part; j++) {
      dag->nodes[i+num_part*3+offset]->parents[j] = dag->nodes[j+num_part*2+offset];
    }
  }

  // set next stage's parents
  if (round_order != LAST) {
    // set parents for the next round
    for (uint32_t i = 0; i < num_part; i++) {
      for (uint32_t j = 0; j < num_part; j++) {
	dag->nodes[i+num_part*3+offset]->children[j] = dag->nodes[i+num_part*4+offset];
	dag->nodes[i+num_part*4+offset]->parents[j] = dag->nodes[i+num_part*3+offset];
      }
    }
  }

  return num_part * 4;
}

uint32_t filter_set_edges(DAG *dag, uint32_t num_part, uint32_t offset, int round_order) {
  if (round_order != LAST) {
    // set parents for the next round
    for (uint32_t i = 0; i < num_part; i++) {
      dag->nodes[i+offset]->children[0] = dag->nodes[i+num_part+offset];
      dag->nodes[i+num_part+offset]->parents[0] = dag->nodes[i+offset];
    }
  }

  return num_part;
}

uint32_t project_set_edges(DAG *dag, uint32_t num_part, uint32_t offset, int round_order) {
  return filter_set_edges(dag, num_part, offset, round_order);
}

uint32_t sort_preprocess_set_edges(DAG *dag, uint32_t num_part, uint32_t offset, int round_order) {
  return filter_set_edges(dag, num_part, offset, round_order);
}

uint32_t agg_set_edges(DAG *dag, uint32_t num_part, uint32_t offset, int round_order) {
  for (uint32_t i = 0; i < num_part; i++) {
    dag->nodes[i+offset]->children[0] = dag->nodes[num_part+1+offset];
  }
  
  for (uint32_t j = 0; j < num_part; j++) {
    dag->nodes[offset+num_part+1]->parents[j] = dag->nodes[j+offset];
    dag->nodes[offset+num_part+1]->children[j] = dag->nodes[j+num_part+1+offset];
  }

  if (round_order != LAST) {
    for (uint32_t i = 0; i < num_part; i++) {
      dag->nodes[i+num_part+1+offset]->parents[0] = dag->nodes[num_part+1+offset];
      for (uint32_t j = 0; j < num_part; j++) {
	dag->nodes[i]->children[j] = dag->nodes[j+num_part*4+1];
      }
    }
  }

  return num_part * 2 + 1;
}

uint32_t join_set_edges(DAG *dag, uint32_t num_part, uint32_t offset, int round_order) {
  for (uint32_t i = 0; i < num_part; i++) {
    dag->nodes[i+offset]->children[0] = dag->nodes[num_part+1+offset];
  }
  
  for (uint32_t j = 0; j < num_part; j++) {
    dag->nodes[offset+num_part+1]->parents[j] = dag->nodes[j+offset];
    dag->nodes[offset+num_part+1]->children[j] = dag->nodes[j+num_part+1+offset];
  }

  if (round_order != LAST) {
    for (uint32_t i = 0; i < num_part; i++) {
      dag->nodes[i+num_part+1+offset]->parents[0] = dag->nodes[num_part+1+offset];
      for (uint32_t j = 0; j < num_part; j++) {
	dag->nodes[i]->children[j] = dag->nodes[j+num_part*4+1];
      }
    }
  }

  return num_part * 2 + 1;
}

uint32_t enc_sort_set_edges(DAG *dag,
			    uint32_t num_part,
			    uint32_t offset,
			    int round_order) {
  // First, sample from each partition
  for (uint32_t i = 0; i < num_part; i++) {
    dag->nodes[i+offset]->children[0] = dag->nodes[num_part+offset];
    dag->nodes[num_part+offset]->parents[i] = dag->nodes[i+offset];
  }

  // send boundaries to everywhere
  for (uint32_t i = 0; i < num_part; i++) {
    dag->nodes[num_part+offset]->children[i] = dag->nodes[i+offset+num_part+1];
    dag->nodes[i+offset+num_part+1]->parents[0] = dag->nodes[num_part+offset];
  }
  
  // using the boundaries, do a local sort and send to every partition
  for (uint32_t i = 0; i < num_part; i++) {
    for (uint32_t j = 0; j < num_part; j++) {
      dag->nodes[i+num_part*2+offset+1]->parents[j] = dag->nodes[j+num_part+offset+1];
    }
  }

  if (round_order != LAST) {
    for (uint32_t i = 0; i < num_part; i++) {
      dag->nodes[i+offset+num_part+2]->parents[0] = dag->nodes[i+num_part+offset+1];
      dag->nodes[i+num_part+offset+1]->children[0] = dag->nodes[i+offset+num_part+2];
    }
  }

  return num_part * 3 + 1;
}

uint32_t enc_agg_set_edges(DAG *dag,
			   uint32_t num_part,
			   uint32_t offset,
			   int round_order) {
  // one pass only
  if (round_order != LAST) {
    // set parents for the next round
    for (uint32_t i = 0; i < num_part; i++) {
      dag->nodes[i+offset]->children[0] = dag->nodes[i+num_part+offset];
      dag->nodes[i+num_part+offset]->parents[0] = dag->nodes[i+offset];
    }
  }

  return num_part;
}

uint32_t enc_join_set_edges(DAG * dag,
			    uint32_t num_part,
			    uint32_t offset,
			    int round_order) {
  return enc_agg_set_edges(dag, num_part, offset, round_order);
}

uint32_t get_benchmark_op_code(uint32_t op_code) {
  (void) op_code;
  return TEST_VERIFY;
}

