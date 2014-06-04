import butter

class FileSink:
	def __init__(self, host, path):
		self.path = path
		self.butter = butter.Butter(path)

	def listVolumes(self):
		''' Return list of volume UUIDs that are available
		'''
		return self.butter.listVolumes()

	def iterEdges(self, fromVol):
		''' Return the edges available from fromVol
			Edge: to UUID, estimated size in MB
		'''
		raise NotImplementedError

