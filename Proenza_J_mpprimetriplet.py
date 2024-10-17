"""
Jacob Proenza-Smith
10/16/2024 (Extension Policy Used)
Assignment 3 
Due 10/17/2024 (extension)
This program uses a commandline input and user input
to create that many worker processes and to find the
smallest prime triplet above the user input. 
All work below was done by Jacob Proenza-Smith
I did not use AI
For a sample input of 1000000000000:
With 1,2,4,8 Processes respectively Recieved
5.7, 4.02, 3.43, 2.9  Seconds.
For a sample input of 3000000000000:
With 1,2,4,8 Processes respectively Revieved
8.84, 7.2, 6.3, 5.3   Seconds.
"""


import math
import time
import argparse
import multiprocessing

primes = [2,3,5]

def makePrimeList(n):
    for i in n:
        if (isPrime(i)):
            primes.append(i)
            
def isPrime(n):
    i=0
    b = int(math.sqrt(n)) + 1
    pLen = len(primes)
    while i < pLen and (primes[i] < b):
        if n % primes[i] == 0:
            return False
        i = i + 1
    #print(f"OUR PRIME NUMBER WE ACCEPT IS : {n}")
    return True

def check_prime_trip(primelist, n):
    if not primelist or len(primelist) < 3:
        return None

    primelist = sorted(set(filter(isPrime, primelist)))

    for i in range(0, len(primelist) - 3):
        if primelist[i] < n:
            continue
        if primelist[i + 2] - primelist[i] == 6:
            if primelist[i + 1] == primelist[i] + 2 or primelist[i + 1] == primelist[i] + 4:
                return (primelist[i], primelist[i + 1], primelist[i + 2])
        elif primelist[i + 3] - primelist[i] == 6:
            if primelist[i+1] - primelist[i] == 2 or primelist[i+1] - primelist[i] == 4:
                return (primelist[i], primelist[i + 1], primelist[i + 3])
            elif primelist[i+2] - primelist[i] == 2 or primelist[i+2] - primelist[i] == 4:
                return (primelist[i], primelist[i + 2], primelist[i + 3])
    return None

def worker(n):
    #print(f"Inside start worker with process number {n}")
    while True:
        l, u = tQueue.get()
        #print(f"Worker: {n}, l = {l}, u = {u}")
        if l < 0 or u < 0:
            break  # Termination signal received
        myPrimes = []
        for i in range(l, u):
            if isPrime(i):
                myPrimes.append(i)
        if myPrimes:
            #print(f"my primes LIST: {myPrimes}")        
            rQueue.put(myPrimes)  # Send the entire list of primes found
    rQueue.put(None)  # Signal this worker has finished
    #print(f"Worker: {n} finished")

def give_work(tQueue, curr, chunk, num_workers):
    for worker_id in range(num_workers):
        start = curr + worker_id * chunk
        end = start + chunk
        print(f"Assigning worker {worker_id} range: {start} to {end}")
        tQueue.put((start, end))

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("num_workers", default=1, nargs='?', type=int, help="Number of child processes")
    args = parser.parse_args()

    n = int(input("Input n: "))
    startT = time.time()

    largestPrimeNeeded = int (math.sqrt(n + 100000))

    if (n <= 5):
        msg = 'The smallest triplet larger than ' + str(n) + ' is ('
        msg = msg + '5, 7, 11)'
        #print(msg)
        exit()

    smallPrime = int(math.sqrt(n)+1)
    for i in range(3, smallPrime):
        if isPrime(i):
            primes.append(i)

    tQueue = multiprocessing.Queue()
    rQueue = multiprocessing.Queue()
    processes = []
    primeTrip = None
    for i in range(0, args.num_workers):
        processes.append(multiprocessing.Process(target=worker, args=(i,)))
    for i in range(0, args.num_workers):
        processes[i].start()
        #print(f"Starting process number {i}")
    m1,m2,m3 = 2,2,2
    curr = n
    chunk = 1000
    while primeTrip is None:
        give_work(tQueue, curr,chunk, args.num_workers)
        primes = [2,3,5]
        for _ in range(0, args.num_workers):
            #print("RQUEUE > GET")
            worker_primes = rQueue.get()
            if worker_primes:
                primes.extend(worker_primes)
            #print(f"worker primes list is DFSDJFDSJKFLKDFJKLSDF {worker_primes}")
        if len(worker_primes) >= 6:
            primes.sort()
            #print(f"Primes GLOBAL LIST IS NOW {primes}")
            primeTrip = check_prime_trip(primes, n)
        if primeTrip is not None:
            break
        curr = curr + args.num_workers * chunk
    for _ in range(args.num_workers):
        tQueue.put((-1, -1))
    endT = time.time()
    for p in processes:
        p.join()
    #print("End")
    print(f" The closest primeTrip is {primeTrip}")
    print(f"it took {endT - startT} seconds")
    