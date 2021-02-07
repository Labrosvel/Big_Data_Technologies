from mrjob.job import MRJob

class MRmyjob(MRJob):
	def mapper(self, _, line):
		data=line.split(", ")
		if len(data)>=2:
			age=data[0].strip()
			occ=data[1].strip()
			yield occ, age

	def reducer(self, key, list_of_values):
		count=0
		total=0.0
		for x in list_of_values:
			total=total+int(x)
			count=count+1
		avg=total/count
		yield key, avg
if __name__== "__main__":
	MRmyjob.run()

