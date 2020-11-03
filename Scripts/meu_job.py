from mrjob.job import MRJob

class ContadorAvaliacoes(MRJob):
    def mapper(self, _, line):
    	(userid, movieid, rating, timestamp) = line.split(',')
        yield movieid,1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
     ContadorAvaliacoes.run()
