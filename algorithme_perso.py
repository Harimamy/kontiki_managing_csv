l, caractr_deal_strong, list_index_int  = ["0","1", "", "3RT", "", "", "fdgd", "12", ""], "", [0, 1, 2, 7]

index = 0
for row in l:
    if row == '':
        l[index] = 'NULL'
    index += 1

for val in l:
    if l.index(val) in list_index_int:
        caractr_deal_strong += "{}, "
    else:
        if val == 'NULL':
            caractr_deal_strong += "{}, "
        else:
            caractr_deal_strong += "\'{}\', "

a = "%s".format(*l), (caractr_deal_strong)
a = caractr_deal_strong.format(*l)
print(a[:-2])

