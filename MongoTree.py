import pymongo
import bson.json_util
from bson import ObjectId
import json

class MongoTree(object):
	def __init__(self, servername, dbname, skel_coll, data_coll=None):
		self.servername = servername
		self.dbname = dbname
		self.skel_coll = skel_coll
		self.data_coll = data_coll

		# connect to database
		try:
			self.db = pymongo.Connection(self.servername)[self.dbname]
			self.skel = self.db[self.skel_coll]
			if self.data_coll:
				self.data = self.db[self.data_coll]
		except pymongo.errors.AutoReconnect:
			return -1

		# make sure skel is indexed on path
		self.ensureIndexes()

	def toObjectId(self, _id):
		'''take a string and return a valid ObjectId'''
		#if we already have an ObjectId just return it
		if _id is None or _id == '':
			return None
		if isinstance(_id, ObjectId):
			return _id
		elif isinstance(_id, basestring):
			#remove any commas
			_id = _id.replace(',','')
			if ObjectId.is_valid(_id):
				return ObjectId(_id)
			else:
				#invalid objectid string
				raise KeyError()
				return -2
		else:
			#invalid object type
			raise TypeError()
			return -1

	def createSkeletonTree(self, newSkelName, dataFrom, dataTo = None, identifier = 'name', childLabel = 'clades', skelKeys = {'branch_length'}):
		'''Take a new data tree and create a skeleton tree (ske) of it from data'''
		try:
			newSkel = self.db[newSkelName]
			dataFrom = self.db[dataFrom]
			if dataTo != None:
				dataTo = self.db[dataTo]
			else:
				dataTo = self.data
		except pymongo.errors.AutoReconnect:
			return -1

		if newSkel.count() != 0:
			raise IndexError()
		for item in dataFrom.find({}):
			newItem = {}
			if identifier in item:
				match = dataTo.find({identifier:item[identifier]})
				if match.count() > 1:
					print "Ambiguous Reference"
					return -1
				elif match.count() == 1:
					newItem['dataLink'] = match[0]['_id']
			if childLabel in item:
				newItem['children'] = item[childLabel]
			for i in skelKeys:
				if i in item:
					newItem[i] = item[i]
			newSkel.insert(newItem)

	def insert(self, locationID, dataID):
		'''insert element into tree, element links to dataID'''
		raise NotImplementedError()

	def delete(self, nodeID):
		'''delete node from tree'''
		raise NotImplementedError()

	def getChildren(self, nodeID):
		'''get all the children of a node, unordered'''
		raise NotImplementedError()

	def getParent(self, nodeID):
		'''get the parent of a node, returned as a string'''
		raise NotImplementedError()

	def getDescendants(self, nodeID):
		'''get all the descendants of a node, returns a list of string ids, each id is a descendant'''
		raise NotImplementedError()

	def getDescendantsCriteria(self, nodeID, keyCriteria=None, outputCriteria=None):
		'''get all descendants but with criteria, returns the mongo object'''
		raise NotImplementedError()

	def getPathToNode(self, nodeID):
		'''get the entire path from nodeID to the node of the tree, return a list of string ids'''
		raise NotImplementedError()

	def ensureIndexes(self, coll=None, indexes='path'):
		'''ensure skel is indexed on path'''
		raise NotImplementedError()

	def getDataLink(self, nodeID):
		'''get the object in data_coll corresponding to the skel_coll'''
		nodeID = self.toObjectId(nodeID)
		if nodeID < 0:
			return -1
		#check if data_coll and skel_coll are the same
		if self.skel_coll == self.data_coll:
			return self.skel.find_one({'_id':nodeID})
		else:
			dataID = self.skel.find_one({'_id':nodeID},{'_id':0,'dataLink':1})['dataLink']
			return self.data.find_one({'_id':dataID})

	def getDataIds(self, nodeID):
		'''convert the skel IDs to the corresponding data IDs'''
		if self.skel_coll == self.data_coll:
			return nodeID
		if isinstance(nodeID, list):
			#NOTE ORDER MAY NOT BE PRESERVED!!!!
			nodeIDList = [self.toObjectId(ident) for ident in nodeID]
			result = self.skel.find({'_id':{'$in':nodeIDList}},{'dataLink':1})
			return [str(_id['dataLink']) for _id in result]
		elif isinstance(nodeID, str):
			return str(self.skel.find_one({'_id':self.toObjectId(nodeID)},{'dataLink':1, '_id':0})['dataLink'])
		else:
			raise TypeError()
			return -1

	def generateCompleteSkeletonTree(self):
		'''Creature a full skeleton tree with branch_length and names if available.'''
		raise NotImplementedError()

	def generateJSONTree(self, keys):
		'''Create a JSON tree from skel tree'''
		raise NotImplementedError()

	def generateFromChildTree(self,ChildTree=None,childLabel='clades',rooted=True,rootID=None):
		'''Create a tree strucutre from a childtree. skeltree must be empty.'''
		raise NotImplementedError()

	def generateFromParentTree(self,ChildTree=None,parentLabel='parents',rooted=True,rootID=None):
		'''Create a tree strucutre from a childtree. skeltree must be empty.'''
		raise NotImplementedError()