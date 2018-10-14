import csv
import random
import string
import sys

word_min_length = 3
word_max_length = 16
treatment_max_length = 32

# Derived from https://github.com/treyhunner/names
first_names = [
  'Mary', 'Patricia', 'Linda', 'Barbara', 'Elizabeth', 'Jennifer', 'Maria',
  'Susan', 'Margaret', 'Dorothy', 'Lisa', 'Nancy', 'Karen', 'Betty', 'Helen',
  'Sandra', 'Donna', 'Carol', 'Ruth', 'Sharon', 'James', 'John', 'Robert',
  'Michael', 'William', 'David', 'Richard', 'Charles', 'Joseph', 'Thomas',
  'Christopher', 'Daniel', 'Paul', 'Mark', 'Donald', 'George', 'Kenneth',
  'Steven', 'Edward', 'Brian']
last_names = [
  'Smith', 'Johnson', 'Williams', 'Jones', 'Brown', 'Davis', 'Miller', 'Wilson',
  'Moore', 'Taylor', 'Anderson', 'Thomas', 'Jackson', 'White', 'Harris',
  'Martin', 'Thompson', 'Garcia', 'Martinez', 'Robinson', 'Clark', 'Rodriguez',
  'Lewis', 'Lee', 'Walker', 'Hall', 'Allen', 'Young', 'Hernandez', 'King',
  'Wright', 'Lopez', 'Hill', 'Scott', 'Green', 'Adams', 'Baker', 'Gonzalez',
  'Nelson', 'Carter']

def randomword():
  return ''.join(random.choice(string.lowercase)
                 for i in range(random.randint(word_min_length, word_max_length)))

def random_name():
  return '%s %s' % (random.choice(first_names), random.choice(last_names))

def main():
  print 'Loading icd_codes.txt...'
  icd_codes = []
  with open('icd_codes.txt', 'r') as f:
    for line in f:
      d_id, d_name = line.split(' ', 1)
      d_name = d_name.strip()
      icd_codes.append([d_id, d_name])

  print 'Loading cpt_treatments.csv...'
  cpt_treatments = []
  with open('cpt_treatments.csv', 'rb') as f:
    r = csv.reader(f)
    for row in r:
      t_id, t_name = row[0], row[1]
      t_name = t_name.strip()
      cpt_treatments.append(t_name)

  num_diseases = len(icd_codes)
  num_genes = num_diseases * 4
  num_treatments = num_diseases * 2

  print 'Generating disease.csv...'
  with open('disease.csv', 'w') as f:
    w = csv.writer(f)
    for d_gene_id, row in enumerate(icd_codes):
      d_id, d_name = tuple(row)
      w.writerow([d_id, d_gene_id, d_name])

  print 'Generating gene.csv...'
  with open('gene.csv', 'w') as f:
    w = csv.writer(f)
    for g_id in range(num_genes):
      g_name = randomword()
      w.writerow([g_id, g_name])

  print 'Generating treatment.csv...'
  with open('treatment.csv', 'w') as f:
    w = csv.writer(f)
    for t_id in range(num_treatments):
      t_disease_id = icd_codes[random.randint(0, len(icd_codes) - 1)][0]
      t_name = random.choice(cpt_treatments)[:treatment_max_length]
      t_cost = random.randint(1, 50000)
      w.writerow([t_id, t_disease_id, t_name, t_cost])

  max_patient_exponent = int(sys.argv[1]) if len(sys.argv) > 1 else 1
  for num_patient_exponent in range(max_patient_exponent):
    num_patients = 125 * 2**num_patient_exponent
    print 'Generating patient-%d.csv...' % (num_patients, )
    with open('patient-%d.csv' % (num_patients, ), 'w') as f:
      w = csv.writer(f)
      for p_id in range(num_patients):
        p_disease_id = icd_codes[random.randint(0, len(icd_codes) - 1)][0]
        p_name = random_name()
        w.writerow([p_id, p_disease_id, p_name])

if __name__ == '__main__':
  main()
