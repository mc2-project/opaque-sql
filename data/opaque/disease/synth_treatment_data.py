import random, string

def randomword(length):
   return ''.join(random.choice(string.lowercase) for i in range(length))

def make_csv():
  disease_codes = []
  with open('icd_codes.csv') as f:
      txt = f.read()
      for line in txt.split('\n'):
        print(line)
        try:
          disease_code, name = line.split(',', 1)
        except ValueError:
          continue
        disease_codes.append(disease_code)


  treatment_id = 0
  write = []
  
  for disease_code in disease_codes:
    for i in range(4):
      write.append(str(treatment_id) + ',' + disease_code + ',' + randomword(random.randint(4, 150)) + ',' + str(random.randint(1, 50000)))
      treatment_id += 1


  with open('treatment.csv', 'w') as f:
    for line in write:
      f.write(line + '\n')

if __name__ == '__main__':
  make_csv()
