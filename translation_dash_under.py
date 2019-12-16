import unittest
import test

# w = 1
# h = 2
# c = 4
# if c and h != '':
#     print("OK")
#
# if h and w != 4:
#     print('Mlay André')

ext = ['.eu', '.cu', '.img']
chaine = "test.eu"
# for ex in ext:
#     if ex in chaine:
#         print("True")
#     else:
#         print("False")

list_cmp_ext = [print("True") if ex in chaine else print("False") for ex in ext]

from collections import Counter

liste = ['a', 'b', 'a', 'b', 'c', 'a', 'b', 'a', 'b', 'c', 'd']

c = Counter(liste)
print(c)
for key, value in c.items():
    if value > 1:
        print(key, " répété {} fois".format(value))
    print(key, value)
