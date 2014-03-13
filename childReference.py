import MongoTree
import pymongo
import pprint

class childReference(MongoTree.MongoTree):
	'''Class for operations on materialized path tree hierachy structures'''

	def insert(self, locationID, dataID):
		'''insert element into tree with child locationID, element links to dataID'''

		try:
			dataID = self.toObjectId(dataID)
			locationID = self.toObjectId(locationID)
		except:
			raise

		return self.skel.insert({'child':locationID, 'dataLink':dataID})

	def delete(self, nodeID):
		'''remove a node from the skeleton tree, leave in the dataID tree'''
		try:
			self.skel.remove({'_id':self.toObjectId(nodeID)})
		except:
			raise

	def getChildren(self, nodeID):
		'''get all the children of a node, returned as a list of strings'''
		children = []
		try:
			child = self.skel.find({'_id':self.toObjectId(nodeID)},{'children':1, '_id':0})
		except:
			raise

		for c in child:
			children.append(str(c))
		return children

		children = []
		try:
			child = self.skel.find({'parent':self.toObjectId(nodeID)},{'_id':1})
		except:
			raise

		for c in child:
			children.append(str(c['_id']))
		return children

	def getParent(self, nodeID):
		'''get the parent of a node, returned as a string'''
		try:
			parent = self.skel.find_one({'children':self.toObjectId(nodeID)},{'_id':1})
		except:
			raise
		if parent:
			return str(parent['_id'])
		else:
			return None

	def getDescendants(self, nodeID):
		'''get all the descendants of a node, returns a list of string ids, each id is a descendant'''
		descendants = []
		stack = []

		try:
			#recursively find all descendants of nodeID
			item = self.skel.find_one({'_id':self.toObjectId(nodeID)},{'_id':1})
			stack.append(item)
			while (stack.length > 0):
				currentNode = stack.pop()
				children = self.skel.find({'children':currentNode['_id']},{'_id':1})
				for c in children:
					descendants.append(str(c['_id']))
					stack.append(c['_id'])
		except:
			raise

		return descendants

	def getPathToNode(self, nodeID):
		'''get the entire path from nodeID to the node of the tree, return a list of string ids'''
		path = [nodeID]
		try:
			item = nodeID
			while (getParent(item) != None):
				item = getParent(item)
				path.append(str(item['_id']))
		except:
			raise

		return path.reverse()

	def ensureIndexes(self, coll=None, indexes='parent'):
		'''ensure skel is indexed on path'''
		if coll is None:
			coll = self.skel_coll
		self.db[coll].ensure_index(indexes)

	def generateCompleteSkeletonTree(self):
		'''Create a complete skeleton tree with branch_length and names if available.'''
		for item in self.skel.find():
			updateDict = {}
			print item
			data = self.data.find_one({'_id':item['dataLink']})
			if 'branch_length' in data:
				updateDict['branch_length'] = data['branch_length']
			if 'name' in data:
				updateDict['name'] = data['name']
			self.skel.update({'_id':item['_id']},{'$set':updateDict})

	def generateJSONTree(self, keys, DataTree=None, SkelTree=None,childDataLabel='clades'):
		'''Generate full JSON Tree'''
		pp = pprint.PrettyPrinter(indent=2)
		JSONArray = []
		keylist = {}
		dataLinkMap = {}
		if DataTree is not None:
			self.data_coll = DataTree
			try:
				print "pymongo.Connection(" + self.servername + ")[" + self.dbname + "][" + self.data_coll + "]"
				self.data = pymongo.Connection(self.servername)[self.dbname][self.data_coll]
			except pymongo.errors.AutoReconnect:
				raise
		if SkelTree is not None:
			self.skel_coll = SkelTree
			try:
				self.skel = pymongo.Connection(self.servername)[self.dbname][self.skel_coll]
			except pymongo.errors.AutoReconnect:
				raise
		for i in keys:
			keylist[i] = 1
		keylist['branch_length'] = 1
		#pp.pprint(keylist)

		skelKeyList = keylist
		skelKeyList['children'] = 1
		skelKeyList['dataLink'] = 1
		for item in self.skel.find({},skelKeyList):
			if 'dataLink' in item:
				data = self.data.find_one({'_id': item['dataLink']},keylist)
				pp.pprint(item['dataLink'])
				if 'children' in item:
					data[childDataLabel] = item['children']
				dataLinkMap[item['_id']] = data['_id']
			else:
				data = item
			JSONArray.append(data)
		for item in JSONArray:
			if childDataLabel in item:
				for child in item[childDataLabel]:
					if child in dataLinkMap:
						child = dataLinkMap[child]
		pp.pprint(JSONArray)
		pp.pprint(dataLinkMap)
		return JSONArray

	def generateFromChildTree(self,ChildTree=None,childLabel='clades',rooted=True,rootID=None):
		'''Create a child tree structure from a childtree. skeltree must be empty.'''
		# collection must be empty to generate tree
		dataLinkMap = {}
		if self.skel.count() != 0:
			raise IndexError()
		if ChildTree is not None:
			self.data_coll = ChildTree
			try:
				self.data = pymongo.Connection(self.servername)[self.dbname][self.data_coll]
			except pymongo.errors.AutoReconnect:
				raise
		for item in self.data.find({},{'_id':True, 'clades':True}):
			#print item
			if childLabel in item:
				id = self.skel.insert({'dataLink':item['_id'], 'children':item['clades']})
			else:
				id = self.skel.insert({'dataLink':item['_id']})
			dataLinkMap[item['_id']] = id
		for item in self.skel.find({'children':{'$exists':True}}, {'children'}):
			for i in xrange(len(item['children'])):
				item['children'][i] = dataLinkMap[item['children'][i]]
			self.skel.update({'_id':item['_id']},{'$set':{'children':item['children']}})

# For testing only
if __name__ == "__main__":

	a = childReference("localhost", "arbor", "SmallTreeSandbox.PhyloTree.SmallTree1SkelChildRef", "SmallTreeSandbox.PhyloTree.SmallTree1")
	#a.createSkeletonTree('SmallTreeSandbox.PhyloTree.SmallTree2Skel','SmallTreeSandbox.PhyloTree.SmallTree2')
	print a.generateJSONTree(['name','_id'],'SmallTreeSandbox.PhyloTree.SmallTree1','SmallTreeSandbox.PhyloTree.SmallTree2Skel')
