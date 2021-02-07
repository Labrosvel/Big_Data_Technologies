#!/usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRmyjob(MRJob):
	def mapper(self, _, line):
		data=line.split(', ')
		if len(data)>=2:
			age=data[0].strip()
			occ=data[1].strip()
			yield (occ,age), 1

	def reducer1(self, key, list_of_values):
		yield key[0], (key[1], sum(list_of_values))
	def reducer2(self, key, list_of_values):
		yield key, max(list_of_values)
	def steps(self):
		return [MRStep(mapper=self.mapper, reducer=self.reducer1),
			MRStep(reducer=self.reducer2)]
		#returns occupation and frequency of maximum
if __name__== "__main__":
	MRmyjob.run()

