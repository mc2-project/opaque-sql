import csv
import random
import string
import sys

name_min_length = 3
name_max_length = 16

def randomword():
  return ''.join(random.choice(string.lowercase)
                 for i in range(random.randint(name_min_length, name_max_length)))

def main():
  print 'Loading icd_codes.txt...'
  icd_codes = []
  with open('icd_codes.txt', 'r') as f:
    for line in f:
      d_id, d_name = line.split(' ', 1)
      d_name = d_name.strip()
      icd_codes.append([d_id, d_name])

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
      t_name = randomword()
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
        p_name = randomword()
        w.writerow([p_id, p_disease_id, p_name])

if __name__ == '__main__':
  main()
