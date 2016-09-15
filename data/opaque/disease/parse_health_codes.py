def make_csv():
  lines = []
  with open('icd_codes.txt') as f:
    txt = f.read()
    for l in txt.split('\n'):
      try:
        print('line: ', l)
        code, name = l.split(' ', 1)
        name = name.lstrip()
        lines.append(code + ',' + name)
      except ValueError:
        continue

  with open('icd_codes.csv', 'w') as f:
    for l in lines:
      f.write(l + '\n')

if __name__ == '__main__':
  make_csv()
