from multiprocessing import Pool, freeze_support, Process
import os, time

def teste(n):
    time.sleep(5)
    print(n)

if __name__ == '__main__':

    print(os.cpu_count())

    # freeze_support()

    # with Pool(os.cpu_count()-1) as pool:
    #     for x in range(20):
    #         p = Process(target=teste, args=(x,))
    #         p.start()
