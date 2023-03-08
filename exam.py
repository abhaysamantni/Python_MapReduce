#!/usr/bin/env python
from mrjob.job import MRJob
class MRWordCount(MRJob):
    def mapper(self, _, line):
        k=0
        for word in line.split():
            k+=1
            yield(word, [1,k])
    def reducer(self, word, temp):
        totaltemp2 = 0.0
        totalcount2 = 0
        for i in temp:
            totaltemp2+=i[0]
            totalcount2+=i[1]
        yield word, totaltemp2/6

if __name__ == '__main__':
    MRWordCount.run()
