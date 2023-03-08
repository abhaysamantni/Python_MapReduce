from mrjob.job import MRJob
import mrjob
import pdb
import math

class MRCount(MRJob):

	def mapper(self, _, line):
		# Each line is CSV
		# Skip header and emit month and count
		l = [s.strip('"') for s in line.split(',')]
		if l[0] != '':
			# strip off quotes
			yield int(l[0]), int(l[2])

	def combiner(self, month, temp):
		i=0;
		totaltemp1=[0.0,0.0]

		for i in temp:
			totaltemp1[0]+=i
			totaltemp1[1]+=1

		yield month, totaltemp1

	def reducer(self, month, temp):
		i=0.0;
		totaltemp2=0.0
		totalcount2=0
		for i in temp:
			totaltemp2+=i[0]
			totalcount2+=i[1]
		yield month, totaltemp2/float(totalcount2)

if __name__ == '__main__':
	MRCount.run()
