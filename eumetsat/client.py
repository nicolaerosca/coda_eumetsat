from __future__ import print_function
import requests
from requests.auth import HTTPBasicAuth
import json
import xml.etree.ElementTree as et
import os
import zipfile


class CodaDataClient:

	auth = None
	base_url = None
	debug = False

	def	__init__(self, user, password, base_url='https://coda.eumetsat.int/', debug=False):
		self.debug = debug
		if(base_url.endswith('/')):
			self.base_url = base_url
		else:
			self.base_url = base_url + '/'
		self.auth = HTTPBasicAuth(user, password)


	def build_query_url(self, polygon, start_date_str, end_date_str, instrument=None):
		georapic_area = ''
		if(len(polygon) > 1):
			polygon_point_str = ','.join((' '.join(str(x) for x in p)) for p in polygon)
			georapic_area = 'POLYGON(({points_str}))'.format(points_str=polygon_point_str)
		else:
			georapic_area = ','.join(str(x) for x in polygon[0])
		query = 'footprint:"Intersects({georapic_area})" ) AND ( beginPosition:[{start_date_str} TO {end_date_str}] AND endPosition:[{start_date_str} TO {end_date_str}]'.format(georapic_area=georapic_area,start_date_str=start_date_str,end_date_str=end_date_str)
		if(instrument):
			query = query + ' AND instrumentshortname:{instrument}'.format(instrument=instrument)
		query_url = '{base_url}search?format=json&orderby=creationdate asc&start=0&rows=100&q=({query})'.format(base_url=self.base_url,query=query)
		return query_url


	def check_product_content(self, file):
		url = '{base_url}/odata/v1/Products(\'{id}\')/Nodes(\'{file_name}\')/Nodes'.format(base_url=self.base_url,id=file['id'],file_name=file['name'])
		if(self.debug):
			print(url)
		response = requests.get(url, auth=self.auth)
		files = []
		if(response.ok):
			data = et.fromstring(response.content)
			ns = {'feed': 'http://www.w3.org/2005/Atom'}
			entries = data.findall('feed:entry', ns)
			#print(data.tag)
			#for child in data:
			#    print(child.tag)
			#print("found " + str(len(entries)))
			for result in entries:
				title=result.find('feed:title', ns).text
				updated=result.find('feed:updated', ns).text
				files.append({'title': title, 'updated': updated})
		else:
			print('Error: ' + str(response))
		return files


	# product_entry: search for specific CDF file name in Nodes of the zip file
	def download_products(self, files, product_entry=None):
		for file in files:
			if(product_entry != None):
				cdf_files = self.check_product_content(file)
				print('Found ' + str(len(cdf_files)) + ' files for product')
				for cdf_file in cdf_files:
					if product_entry in cdf_file['title']:
						file['cdf_file'] = cdf_file['title']
						self.download(file)
			else:
				self.download(file) 

	"""
	returns list of products names, id and summary

	polygon: array of points with longitude and latitude coordinates
	beginDate:
	endDate:
	instrument: Possible options are: SAR, MSI, OLCI, SLSTR, SRAL
	""" 
	def query(self, polygon, start_date_str, end_date_str, instrument=None):
		url= self.build_query_url(polygon=polygon, start_date_str=start_date_str, end_date_str=end_date_str, instrument=instrument)
		if(self.debug):
			print(url)
		response = requests.get(url, auth=self.auth)
		# For successful API call, response code will be 200 (OK)
		files = []
		if(response.ok):
			data = json.loads(response.content)
			if(self.debug):
				print('Query EUMETSAT....')
				print(data['feed']['opensearch:Query']['searchTerms'])
				print('Result size:' + data['feed']['opensearch:totalResults'])
			
			if 'entry' in data['feed']:
				for result in data['feed']['entry']:
					id=result['id']
					file_name=result['title']
					for extr_param in result['str']:
						if(extr_param['name'] == 'filename'):
							file_name = extr_param['content']
					summary=result['summary']
					files.append({'name': file_name, 'summary': summary, 'id': id})
		else:
			print(response)
		return files


	

	def download(self, file):
		if('cdf_file' in file):
			url = '{base_url}odata/v1/Products(\'{id}\')/Nodes(\'{file_name}\')/Nodes(\'{cdf_file}\')/$value'.format(base_url=self.base_url,id=file['id'],file_name=file['name'],cdf_file=file['cdf_file'])
			# create folder
			self.save_local_file(url=url, local_filename=file['name'] + '/' + file['cdf_file'])
		else:
			url = '{base_url}odata/v1/Products(\'{id}\')/Nodes(\'{file_name}\')/$value'.format(base_url=self.base_url,id=file['id'],file_name=file['name'])
			local_file = self.save_local_file(url=url, local_filename=file['name'] + '.zip')
			if local_file:
				print("unzip file " + local_file)
				zip_ref = zipfile.ZipFile(local_file, 'r')
				zip_ref.extractall(file['name'])
				zip_ref.close()


	def save_local_file(self, url, local_filename):
		if(self.debug):
			print('donwloading file: ' + local_filename)
			print(url)
		r = requests.get(url,auth=self.auth, stream=True)
		if(r.ok):
			# check file name contains folder 
			if '/' in local_filename:
				directory = local_filename[0:local_filename.index('/')]
				if not os.path.exists(directory):
					os.makedirs(directory)
			if not os.path.exists(local_filename):
				with open(local_filename, 'wb') as f:
					for chunk in r.iter_content(chunk_size=1024): 
						if chunk:
							f.write(chunk)
				return local_filename
			else:
				if(self.debug):
					print('Fille already exist.')
		else:
			print('Error: ' + str(r))


