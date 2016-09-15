import random, string, sys

def randomword(length):
   return ''.join(random.choice(string.lowercase) for i in range(length))

def make_csv():
  disease_codes = []
  with open('icd_codes.csv') as f:
      txt = f.read()
      for line in txt.split('\n'):
        try:
          disease_code, name = line.split(',', 1)
        except ValueError:
          continue
        disease_codes.append(disease_code)


  patient_id = 0
  write = []

  num_patients = int(sys.argv[1])
  for i in range(num_patients):
      disease_code = disease_codes[random.randint(0, len(disease_codes) - 1)]
      write.append(str(patient_id) + ',' + disease_code + ',' + randomword(random.randint(3, 150)))
      patient_id += 1


  with open('patient-%d.csv' % (num_patients,), 'w') as f:
    for line in write:
      f.write(line + '\n')

if __name__ == '__main__':
  make_csv()
