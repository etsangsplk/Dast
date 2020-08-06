#!/usr/bin/python3
import sys
import subprocess
from itertools import combinations

# input 
# site_names = [["S1A", "S1B", "S1C"], ["S2A", "S2B", "S2C"]]
site_names = [["S1A", "S1B", "S1C"], ["S2A", "S2B", "S2C"], ["S3A", "S3B", "S3C"]]
# ends here



exe_orders = []

def GrepExe():
    for par in site_names:
        for site in par:
            filename = "log/proc-%s.log" % site
            subprocess.run("cat %s | grep Going | grep First | cut -d '=' -f2 > log/proc-%s.serial" % (filename, site), shell= True)

def ReadOrders():
    for par in site_names:
        par_orders = []
        for site in par:
            filename = "log/proc-%s.serial" % site
            site_order = []
            with open(filename) as f:
                for line in f:
                    site_order.append(int(line))
            par_orders.append(site_order)
        exe_orders.append(par_orders)

def CheckParIdentical():
    for index in range(len(exe_orders)):
        par_orders = exe_orders[index]
        i = 1
        while (i < len(par_orders)):
            if (par_orders[0] != par_orders[i]):
                print("Warning!! the execution of %s and %s is not identical" % (site_names[index][0], site_names[index][i]))
                min_len = min([len(par_orders[0]), len(par_orders[i])])
                for j in range(min_len):
                    if par_orders[0][j] != par_orders[i][j]:
                        print("ERROR differenece: %d vs %d in the %d-th" % (par_orders[0][j], par_orders[i][j], j))
                        sys.exit(-1)
                if len(par_orders[0]) > min_len:
                   print("the execution of %s is shorter than %s for %d txns" % (site_names[index][i], site_names[index][0], len(par_orders[0]) - min_len))
                if len(par_orders[i]) > min_len:
                   print("the execution of %s is shorter than %s for %d txns" % (site_names[index][0], site_names[index][i], len(par_orders[i]) - min_len))
            i = i + 1
def CheckCrossParSerializable():
    positions = []
    for par_id in range(len(site_names)):
        par_pos = {}
        # checked identical, use the first one
        order = exe_orders[par_id][0]
        for i in range(len(order)):
            if order[i] in par_pos:
                print("ERROR!! redundant exectuion of txn %d in site %s" % (order[i], site_names[par_id][0]))
                sys.exit(-1)
            else:
                par_pos[order[i]] = i 
        positions.append(par_pos)

    c = combinations(list(range(len(site_names))), 2)
    for pick in c:
        first_par = pick[0]        
        second_par = pick[1]
        cur = -1
        for txn in exe_orders[second_par][0]:
            p = positions[first_par].get(txn)
            if p != None:
                if not p > cur:
                    print("ERROR!! inconsisitent execution of %s and %s in site %s and site %s" % (txn, exe_orders[first_par][0][cur], site_names[first_par][0] , site_names[second_par][0]))
                    sys.exit(-2)
                else:
                    cur = p
        print("Congrats, the execution of par %d and %d are serializable" % (first_par, second_par))

if __name__ == "__main__":
    GrepExe()
    ReadOrders()
    CheckParIdentical()
    CheckCrossParSerializable()
   
    # read the files 
    # file1 = sys.argv[1]
    # array1 = []
    # with open(file1) as f:
    #    for line in f:
    #        array1.append(int(line))

    # file2 = sys.argv[2]
    # array2 = []
    # with open(file2) as f:
    #     for line in f: 
    #         array2.append(int(line))
    
    # # pass seq 1 
    # pos1 = {}
    # for i in range(len(array1)):
    #     if array1[i] in pos1:
    #         print("ERROR!! redundant exectuion of", array1[i])
    #         sys.exit(-1)
    #     else: 
    #         # remember the position of 
    #         pos1[array1[i]] = i

    # cur = -1
    # for exe in array2:
    #     p = pos1.get(exe)
    #     if p != None:
    #         # exist
    #         if not p > cur:
    #             # different order
    #             print("ERROR!! inconsisitent execution of", array1[cur], "and", exe)
    #             sys.exit(-2)
    #         else:
    #             cur = p
    
    # print("check success, the execution of %s and %s are serializable" % (file1, file2))

