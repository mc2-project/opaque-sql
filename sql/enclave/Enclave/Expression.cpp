#include "Expression.h"

// This file contains a set of expressions used for benchmarks
// Basically a set of UDFs


// This expression returns a new string with that is a prefix of str
// returned = str[1:offset]
void prefix(String *input, uint32_t offset, String *result) {
  result->alloc(NULL);
  cpy(result->data, input->data, offset);
  result->length = offset;
}

void sum_integers(Integer **integers, uint32_t num_values,
                  Integer *result) {
  int ret = 0;
  for (uint32_t i = 0; i < num_values; i++) {
    ret += integers[i]->value;
  }

  result->value =ret;
}

// given a list of attributes, return the comparison
void evaluate_join(GenericType **input_attr, uint32_t num_input_attr,
                   GenericType **output_attr, uint32_t num_output_attr,
                   uint32_t expression) {
  switch (expression) {
  case IDENTITY:
  {
    // don't have to copy
    for (uint32_t i = 0; i < num_output_attr; i++) {
      if (dynamic_cast<Integer *>(input_attr[i]) != NULL) {
        dynamic_cast<Integer *>(output_attr[i])->copy_attr(dynamic_cast<Integer *>(input_attr[i]));
      } else if (dynamic_cast<String *>(input_attr[i]) != NULL) {
        dynamic_cast<String *>(output_attr[i])->copy_attr(dynamic_cast<String *>(input_attr[i]), NO_COPY);
      } else {
        printf("evaluate_join: Attribute %d has unknown type\n", i);
        input_attr[i]->print();
        assert(false);
      }
    }
  }
  break;
  default:
    printf("evaluate_join: Unknown expression %d\n", expression);
    assert(false);
    break;
  }

}

void evaluate_agg(GenericType **input_attr, uint32_t num_input_attr,
                  GenericType **output_attr, uint32_t num_output_attr,
                  uint32_t expression) {

  switch (expression) {
  case IDENTITY:
  {
    // don't have to copy
    for (uint32_t i = 0; i < num_output_attr; i++) {
      if (dynamic_cast<Integer *>(input_attr[i]) != NULL) {
        dynamic_cast<Integer *>(output_attr[i])->copy_attr(dynamic_cast<Integer *>(input_attr[i]));
      } else if (dynamic_cast<String *>(input_attr[i]) != NULL) {
        dynamic_cast<String *>(output_attr[i])->copy_attr(dynamic_cast<String *>(input_attr[i]), NO_COPY);
      } else if (dynamic_cast<Float *>(input_attr[i]) != NULL) {
        dynamic_cast<Float *>(output_attr[i])->copy_attr(dynamic_cast<Float *>(input_attr[i]));
      } else {
        printf("evaluate_agg: Attribute %d has unknown type\n", i);
        input_attr[i]->print();
        assert(false);
      }
    }
  }
  break;
  default:
    printf("evaluate_agg: Unknown expression %d\n", expression);
    assert(false);
    break;
  }

}


void evaluate_agg_agg(GenericType **input_attr, uint32_t num_input_attr,
                      GenericType **output_attr, uint32_t num_output_attr,
                      uint32_t expression) {

  switch (expression) {
  case IDENTITY:
  {
    // don't have to copy
    for (uint32_t i = 0; i < num_output_attr; i++) {
      if (dynamic_cast<Integer *>(input_attr[i]) != NULL) {
        dynamic_cast<Integer *>(output_attr[i])->copy_attr(dynamic_cast<Integer *>(input_attr[i]));
      } else if (dynamic_cast<String *>(input_attr[i]) != NULL) {
        dynamic_cast<String *>(output_attr[i])->copy_attr(dynamic_cast<String *>(input_attr[i]), NO_COPY);
      } else if (dynamic_cast<Float *>(input_attr[i]) != NULL) {
        dynamic_cast<Float *>(output_attr[i])->copy_attr(dynamic_cast<Float *>(input_attr[i]));
      } else {
        printf("evaluate_agg_agg: Attribute %d has unknown type\n", i);
        input_attr[i]->print();
        assert(false);
      }
    }
  }
  break;
  default:
    printf("evaluate_agg_agg: Unknown expression %d\n", expression);
    assert(false);
    break;
  }

}


void evaluate_sort(GenericType **input_attr, uint32_t num_input_attr,
                   GenericType **output_attr, uint32_t num_output_attr,
                   uint32_t expression) {
  switch(expression) {
  case IDENTITY:
  {

    // don't have to copy
    for (uint32_t i = 0; i < num_output_attr; i++) {
      if (dynamic_cast<Integer *>(input_attr[i]) != NULL) {
        dynamic_cast<Integer *>(output_attr[i])->copy_attr(dynamic_cast<Integer *>(input_attr[i]));
      } else if (dynamic_cast<String *>(input_attr[i]) != NULL) {
        dynamic_cast<String *>(output_attr[i])->copy_attr(dynamic_cast<String *>(input_attr[i]), NO_COPY);
      } else if (dynamic_cast<Float *>(input_attr[i]) != NULL) {
        dynamic_cast<Float *>(output_attr[i])->copy_attr(dynamic_cast<Float *>(input_attr[i]));
      } else if (dynamic_cast<Dummy *>(input_attr[i]) != NULL) {
        // Do nothing - this case only happens for the dummy sorting opcodes,
        // in which case SortAttributes::compare will take care of sorting
        // dummies
      } else {
        printf("evaluate_sort: Attribute %d has unknown type\n", i);
        input_attr[i]->print();
        assert(false);
      }

    }
  }
  break;
  default:
    printf("evaluate_sort: Unknown expression %d\n", expression);
    assert(false);
    break;
  }
}

void evaluate_project(GenericType **input_attr, uint32_t num_input_attr,
                      GenericType **output_attr, uint32_t num_output_attr,
                      uint32_t expression) {
  switch(expression) {
  case BD2:
  {
    prefix(dynamic_cast<String *>(input_attr[0]),
           8,
           dynamic_cast<String *>(output_attr[0]));
    dynamic_cast<Float *>(output_attr[1])->copy_attr(dynamic_cast<Float *>(input_attr[1]));
  }
  break;
  case PR_WEIGHT_RANK:
  {
    // project identity
    Integer *int_input = dynamic_cast<Integer *>(input_attr[1]);
    Integer *int_output = dynamic_cast<Integer *>(output_attr[0]);

    if (int_input == NULL || int_output == NULL) {
      printf("evaluate_project[PR_WEIGHT_RANK]: dynamic cast into integer returns null\n");
    }

    int_output->copy_attr(int_input);

    // page rank should multiple two attributes together
    Float *v1 = dynamic_cast<Float *>(input_attr[0]);
    Float *v2 = dynamic_cast<Float *>(input_attr[2]);
    Float *v3 = dynamic_cast<Float *>(output_attr[1]);

    if (v1 == NULL || v2 == NULL || v3 == NULL) {
      printf("evaluate_project[PR_WEIGHT_RANK]: dynamic cast into float returns null\n");
      assert(false);
    }
    // TODO: put the multiplication function inside the class
    v3->value = v1->value * v2->value;
  }
  break;

  case PR_APPLY_INCOMING_RANK:
  {
    // project identity
    Integer *int_input = dynamic_cast<Integer *>(input_attr[0]);
    Integer *int_output = dynamic_cast<Integer *>(output_attr[0]);

    if (int_input == NULL || int_output == NULL) {
      printf("evaluate_project[PR_WEIGHT_RANK]: dynamic cast into integer returns null\n");
    }

    int_output->copy_attr(int_input);

    Float *v1 = dynamic_cast<Float *>(input_attr[1]);
    Float *v2 = dynamic_cast<Float *>(output_attr[1]);

    if (v1 == NULL || v2 == NULL) {
      printf("evaluate_project[PR_WEIGHT_RANK]: dynamic cast into float is null\n");
      assert(false);
    }
    // TODO: put the multiplication function inside the class
    v2->value = v1->value * 0.85 + 0.15;
  }
  break;

  default:
    printf("evaluate_project: Unknown expression %d\n", expression);
    assert(false);
    break;
  }
}

void evaluate_expr(GenericType **input_attr, uint32_t num_input_attr,
                   GenericType **output_attr, uint32_t num_output_attr,
                   uint32_t expression,
                   int mode) {

  if (mode == JOIN) {
    evaluate_join(input_attr, num_input_attr, output_attr, num_output_attr,
                  expression);

  } else if (mode == PROJECT) {
    evaluate_project(input_attr, num_input_attr, output_attr, num_output_attr,
                     expression);

  } else if (mode == AGG) {
    evaluate_agg(input_attr, num_input_attr, output_attr, num_output_attr,
                 expression);

  } else if (mode == SORT) {
    evaluate_sort(input_attr, num_input_attr, output_attr, num_output_attr,
                  expression);

  } else if (mode == AGG_AGG) {
    evaluate_agg_agg(input_attr, num_input_attr, output_attr, num_output_attr,
                     expression);
  } else {
    printf("evaluate_expr: Unknown mode %d\n", mode);
    assert(false);
  }
}
