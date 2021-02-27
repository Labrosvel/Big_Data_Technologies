
from mrjob.job import MRJob

class MRmyjob(MRJob):
	def mapper(self, _, line):
		data=line.strip().split(' ') # strips each line from leading or trailing space_bars. Then splits the line using space_bar as delimiter
		line_id = data[0].strip() # assign line_id to the first element of each line
		list_of_symbols = data[1:] # assign list_of_symbols to the remaining (2nd to last) elements of each line
		for symbol in list_of_symbols: # for each line of the dataset imported, returns as key each symbol and as value its corresponding line_id. We create numerous duplicates
			yield symbol, int(line_id) 	
	
	def reducer(self, symbol, line_id):
		inv_index=[] # Create an empty list to populate with line_ids
		[inv_index.append(id) for id in line_id] # Aggregate the values (line_ids) for each key, by passing them in the list defined above
		inv_index = set(inv_index) # Keeping only the unique values for each list (inv_index) 
		yield symbol, sorted(list(inv_index)) # Returning as a key-value pair each symbol and a list of the unique line_ids in which it appears 
		
if __name__ == '__main__':
	MRmyjob.run()
