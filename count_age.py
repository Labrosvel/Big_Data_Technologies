from mrjob.job import MRJob

class MRmyjob(MRJob):
	def mapper(self, _, line):
		data=line.split(', ')
		age=data[0].strip()
		yield age, 1
	def reducer(self, key, list_of_values):
		yield key, sum(list_of_values)
if __name__ == '__main__':
	MRmyjob.run()
