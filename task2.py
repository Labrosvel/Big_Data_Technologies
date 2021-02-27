#Importing packages
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRmyjob(MRJob):
	def mapper(self, _, line): #Empty key and line as value
		data=line.split(', ') #spliting for each 'comma' as delimiter
		age=data[0].strip() #First element of the line is the 'age'
		yield age,1         # outputs age and 1 as key-value pair
		
	def reducer1(self, age, counts):
			yield None, (sum(counts),age) #aggregation of counts for each age, returns tuple of sum() and age as list_of_values
	
	def reducer2(self, _, frequency_age):
		for frequency, age in sorted(frequency_age, reverse=True)[:10]: #sorts tuple descending according to frequency
			yield (int(frequency), age) #returns key (no-value) tuple of frequency - corresponding age

	def steps(self):
		return [MRStep(mapper=self.mapper, reducer=self.reducer1), #define order of reducers
			MRStep(reducer=self.reducer2)]
		
if __name__ == '__main__':
	MRmyjob.run()
