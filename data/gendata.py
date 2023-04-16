import csv
import random


planSz = 100
kwSz = 128




with open('plan.csv', 'w') as f:
    plan = csv.writer(f, delimiter='\t', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    for i in range(PlanSz):
        plan.writerow([i, ",".join([str(random.randint(0,23)) for _ in range(random.randint(0, 24))]), random.randint(0, 1)])

with open('unit.csv', 'w') as f:
    unit = csv.writer(f, delimiter='\t', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    for i in range(20):
        unit.writerow([i, random.randint(1,24), random.randint(1, productSz), genUnitKV()])

with open('prod.csv', 'w') as f:
    proc = csv.writer(f, delimiter='\t', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    for i in range(20):
        prod.writerow([i, random.randint(1, kwSz), genProdMat()])


