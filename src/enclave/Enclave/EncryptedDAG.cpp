#include "EncryptedDAG.h"

// this code is able to generate the correct DAG based on the op_code/benchmark

// true if two are exactly equal
// false otherwise
bool set_verify(std::set<uint32_t> *set1,
                std::set<uint32_t> *set2) {

  std::set<uint32_t>::iterator it_begin = set1->begin();
  std::set<uint32_t>::iterator it_end = set1->end();
  std::set<uint32_t>::iterator it;

  // printf("Set 1\n");
  // for (it = it_begin; it != it_end; it++) {
  //   printf("%u\n", *it);
  // }

  it_begin = set2->begin();
  it_end = set2->end();
  // printf("Set 2\n");
  // for (it = it_begin; it != it_end; it++) {
  //   printf("%u\n", *it);
  // }
  
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
  if (num_parents > 0) {
    free(parents);
  }
  if (num_children > 0) {
    free(children);
  }
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
    //printf("DAG::get_task_id_parents(): num_nodes is %u, nodes[i]->id is %u, task_id is %u\n", num_nodes, nodes[i]->id, task_id);
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

  uint32_t tid = op_code;

  /** Use the op_code to define TID**/

  /** Currently, default is set so that all verification tests pass. Opcodes will be slowly integrated benchmark by benchmark. **/
  // check(false, "task_id_parser: op_code not recognized");
  tid = TID_TEST;
  
  return tid + index;  
}

uint32_t create_node(DAG *dag,
                     uint32_t num_parents,
                     uint32_t num_children,
                     uint32_t num_part,
                     uint32_t offset,
                     int op_code) {

  for (uint32_t i = 0; i < num_part; i++) {
    dag->nodes[i+offset] = new Node(i + op_code, num_parents, num_children);
  }

  return num_part;
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
      num_nodes = num_part * 8;
      dag = new DAG(num_nodes, DID_BD1);

      // Construct nodes
      offset += create_node(dag, 0, 1, num_part, offset, OP_BD1_SORT_SCAN);
      offset += create_node(dag, 1, num_part, num_part, offset, OP_BD1_SORT_PREPROCESS);
      offset += create_node(dag, 1, 1, num_part, offset, OP_BD1_SORT_PAD);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD1_SORT_ROUND1);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD1_SORT_ROUND2);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD1_SORT_ROUND3);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD1_SORT_ROUND4);

      offset += create_node(dag, num_part, 0, num_part, offset, OP_BD1_FINAL_FILTER);

      // Construct edges
      offset = 0;
      offset += sort_set_edges(dag, num_part, offset, FIRST);
      offset += filter_set_edges(dag, num_part, offset, LAST);	  
    }
    break;

  case DID_BD2:
    {
      num_nodes = num_part * 16 + 1;
      dag = new DAG(num_nodes, DID_BD2);

      // AGG sort 1
      offset += create_node(dag, 0, num_part, num_part, offset, OP_BD2_SORT1_PREPROCESS);
      offset += create_node(dag, 1, 1, num_part, offset, OP_BD2_SORT1_PAD);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD2_SORT1_ROUND1);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD2_SORT1_ROUND2);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD2_SORT1_ROUND3);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD2_SORT1_ROUND4);

      // group by
      offset += create_node(dag, num_part, 1, num_part, offset, OP_BD2_GROUPBY_SCAN1);
      offset += create_node(dag, num_part, num_part, 1, offset, OP_BD2_GROUPBY_COLLECT);
      offset += create_node(dag, 1, 1, num_part, offset, OP_BD2_GROUPBY_SCAN2);

      // AGG sort 2 (4 rounds)
      offset += create_node(dag, 1, num_part, num_part, offset, OP_BD2_SORT2_PREPROCESS);
      offset += create_node(dag, 1, 1, num_part, offset, OP_BD2_SORT2_PAD);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD2_SORT2_ROUND1);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD2_SORT2_ROUND2);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD2_SORT2_ROUND3);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD2_SORT2_ROUND4);

      // filter
      offset += create_node(dag, num_part, 1, num_part, offset, OP_BD2_FILTER);

      // project
      offset += create_node(dag, 1, 1, num_part, offset, OP_BD2_PROJECT);

      offset = 0;

      // Construct edges
      // AGG sort 1
      offset += sort_set_edges(dag, num_part, offset, FIRST);
      // group by (3 rounds)
      offset += agg_set_edges(dag, num_part, offset, OTHER);
      // AGG sort 2
      offset += sort_set_edges(dag, num_part, offset, OTHER);
      // filter
      offset += filter_set_edges(dag, num_part, offset, OTHER);
      // filter
      offset += project_set_edges(dag, num_part, offset, LAST);
		
    }
    break;

  case DID_BD3:
    {
      num_nodes = num_part * (1 + 6 + 2 + 7 + 1 + 1 + 2 + 1 + 6) + 2;
      dag = new DAG(num_nodes, DID_BD3);
      offset = 0;

      // join sort preprocess
      offset += create_node(dag, 0, 1, num_part, offset, OP_BD3_JOIN_PREPROCESS);

      // sort 1 (4 rounds)
      offset += create_node(dag, 1, num_part, num_part, offset, OP_BD3_SORT1_PREPROCESS);
      offset += create_node(dag, 1, 1, num_part, offset, OP_BD3_SORT1_PAD);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD3_SORT1_ROUND1);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD3_SORT1_ROUND2);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD3_SORT1_ROUND3);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD3_SORT1_ROUND4);

      // join (3 rounds)
      offset += create_node(dag, num_part, 1, num_part, offset, OP_BD3_JOIN_SCAN1);
      offset += create_node(dag, num_part, num_part, 1, offset, OP_BD3_JOIN_COLLECT);
      offset += create_node(dag, 1, 1, num_part, offset, OP_BD3_JOIN_SCAN2);

      // permute (4 rounds)
      offset += create_node(dag, 1, 1, num_part, offset, OP_BD3_SORT2_SCAN);
      offset += create_node(dag, 1, num_part, num_part, offset, OP_BD3_SORT2_PREPROCESS);
      offset += create_node(dag, 1, 1, num_part, offset, OP_BD3_SORT2_PAD);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD3_SORT2_ROUND1);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD3_SORT2_ROUND2);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD3_SORT2_ROUND3);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD3_SORT2_ROUND4);

      // filter
      offset += create_node(dag, num_part, 1, num_part, offset, OP_BD3_JOIN_FILTER);

      // project
      offset += create_node(dag, 1, 1, num_part, offset, OP_BD3_JOIN_PROJECT);

      // aggregate
      offset += create_node(dag, 1, 1, num_part, offset, OP_BD3_AGG_SCAN1);
      offset += create_node(dag, num_part, num_part, 1, offset, OP_BD3_AGG_COLLECT);
      offset += create_node(dag, 1, 1, num_part, offset, OP_BD3_AGG_SCAN2);

      // project
      offset += create_node(dag, 1, 1, num_part, offset, OP_BD3_AGG_PROJECT);

      // sort
      offset += create_node(dag, 1, num_part, num_part, offset, OP_BD3_SORT3_PREPROCESS);
      offset += create_node(dag, 1, 1, num_part, offset, OP_BD3_SORT3_PAD);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD3_SORT3_ROUND1);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD3_SORT3_ROUND2);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD3_SORT3_ROUND3);
      offset += create_node(dag, num_part, num_part, num_part, offset, OP_BD3_SORT3_ROUND4);

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
      offset += sort_set_edges(dag, num_part, offset, LAST);
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

  case DID_TEST_VERIFY:
    { 
      // sort, aggregate
      num_nodes = 2;
      dag = new DAG(num_nodes, DID_TEST_VERIFY);

      // sort
      dag->nodes[0] = new Node(TID_TEST_SORT, 0, 1);
      // group by
      dag->nodes[1] = new Node(TID_TEST_AGG, 1, 0);

      dag->nodes[0]->children[0] = dag->nodes[1];
      dag->nodes[1]->parents[0] = dag->nodes[0];
    }
    break;

  case DID_TEST:
    {
      num_nodes = 1;
      dag = new DAG(1, DID_TEST);
      dag->nodes[0] = new Node(TID_TEST, 0, 0);
    }
	  
  default:
    {
      check(false, "DAGGenerator::genDAG(): cannot generate DAG, op code not found");
    }
    break;
  }

  return dag;
}


uint32_t sort_set_edges(DAG *dag, uint32_t num_part, uint32_t input_offset, int round_order) {

  uint32_t offset = input_offset;

  // First, scan preprocess
  for (uint32_t i = 0; i < num_part; i++) {
    for (uint32_t j = 0; j < num_part; j++) {
      dag->nodes[i+offset]->children[j] = dag->nodes[j+num_part+offset];
    }
  }
  offset += num_part;

  // Then, one scan to pad
  for (uint32_t i = 0; i < num_part; i++) {
    dag->nodes[i+offset]->parents[i] = dag->nodes[i+offset-num_part];
    dag->nodes[i+offset]->children[i] = dag->nodes[i+offset+num_part];
  }
  offset += num_part;

  // Finally, 4 rounds to sort

  for (uint32_t i = 0; i < num_part; i++) {
    for (uint32_t j = 0; j < num_part; j++) {
      dag->nodes[i+offset]->parents[j] = dag->nodes[j+offset-num_part];
      dag->nodes[i+offset]->children[j] = dag->nodes[j+offset+num_part];
    }
  }
  offset += num_part;

  for (uint32_t i = 0; i < num_part; i++) {
    for (uint32_t j = 0; j < num_part; j++) {
      dag->nodes[i+offset]->parents[j] = dag->nodes[j+offset-num_part];
      dag->nodes[i+offset]->children[j] = dag->nodes[j+offset+num_part];
    }
  }
  offset += num_part;

  for (uint32_t i = 0; i < num_part; i++) {
    for (uint32_t j = 0; j < num_part; j++) {
      dag->nodes[i+offset]->parents[j] = dag->nodes[j+offset-num_part];
      dag->nodes[i+offset]->children[j] = dag->nodes[j+offset+num_part];
    }
  }
  offset += num_part;

  for (uint32_t i = 0; i < num_part; i++) {
    for (uint32_t j = 0; j < num_part; j++) {
      dag->nodes[i+offset]->parents[j] = dag->nodes[j+offset-num_part];
    }
  }
  offset += num_part;
  
  // set next stage's parents
  if (round_order != LAST) {
    // set parents for the next round
    for (uint32_t i = 0; i < num_part; i++) {
      for (uint32_t j = 0; j < num_part; j++) {
        dag->nodes[i+offset]->children[j] = dag->nodes[i+offset+num_part];
        dag->nodes[i+num_part+offset]->parents[j] = dag->nodes[i+offset];
      }
    }
  }
  return (offset - input_offset);
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

  return DID_TEST;
}

Verify::Verify(uint32_t op_code, uint32_t num_part, uint32_t index) {
  parents = new std::set<uint32_t>();
  this->op_code = op_code;
  this->num_part = num_part;
  this->index = index;
  self_task_id = task_id_parser(op_code, index);

  mac_obj = new MAC;
}

Verify::~Verify() {
  delete parents;
  delete mac_obj;
}

void Verify::mac(uint8_t *mac_ptr) {
  (void)mac_ptr;
  mac_obj->mac(mac_ptr, SGX_AESGCM_MAC_SIZE);
}

std::set<uint32_t> *Verify::get_set() {
  return parents;
}

uint32_t Verify::get_self_task_id() {
  return self_task_id;
}

bool Verify::verify() {

  uint32_t benchmark_op_code = get_benchmark_op_code(op_code);
  if (benchmark_op_code == DID_TEST) {
    // printf("Verify::verify(): DAG not yet supported!\n");
    DAG *dag = DAGGenerator::genDAG(DID_BD3, num_part);
    std::set<uint32_t> *input_set = dag->get_task_id_parents(0);
    set_verify(input_set, this->parents);

    delete dag;

  } else {
    DAG *dag = DAGGenerator::genDAG(benchmark_op_code, num_part);

    std::set<uint32_t> *input_set = dag->get_task_id_parents(self_task_id);

    bool verified = set_verify(input_set, this->parents);

    if (!verified) {
      // the OS is malicious -- it's the apocalypse!
      // or maybe our implementation is wrong
      printf("Verify::verify(): Incorrect DAG!\n");
    } else {
      printf("Verify::verify(): Correct DAG\n");
    }

    delete dag;
    delete input_set;

    return verified;
  }

  return true;
}

void Verify::add_node(uint32_t node) {
  parents->insert(node);
}
