import requests
from requests.auth import HTTPBasicAuth
import json
import xml.etree.ElementTree as et
import os


class EumetsatDataClient:

	auth = None
	base_url = None

	def	__init__(self, user, password, base_url='https://coda.eumetsat.int/'):
		if(base_url.endswith('/')):
			self.base_url = base_url
		else:
			self.base_url = base_url + '/'
		self.auth = HTTPBasicAuth(user, password)


	def build_query_url(self, polygon, start_date_str, end_date_str):
		polygon_str = ','.join((' '.join(str(x) for x in p)) for p in polygon)
		query_url = '{base_url}search?format=json&orderby=creationdate asc&start=0&rows=100&q=( footprint:"Intersects(POLYGON(({polygon_str})))" ) AND ( beginPosition:[{start_date_str} TO {end_date_str}] AND endPosition:[{start_date_str} TO {end_date_str}])'.format(base_url=self.base_url,polygon_str=polygon_str,start_date_str=start_date_str,end_date_str=end_date_str)
		return query_url


	def check_product_content(self, file):
		url = '{base_url}/odata/v1/Products(\'{id}\')/Nodes(\'{file_name}\')/Nodes'.format(base_url=self.base_url,id=file['id'],file_name=file['name'])
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
				print('Found ' + str(len(cdf_files)) + ' files')
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
	""" 
	def query(self, polygon, start_date_str, end_date_str, debug=True):
		url= self.build_query_url(polygon=polygon, start_date_str=start_date_str, end_date_str=end_date_str)
		response = requests.get(url, auth=self.auth)
		# For successful API call, response code will be 200 (OK)
		files = []
		if(response.ok):
			data = json.loads(response.content)
			if(debug):
				print('Query EUMETSAT....')
				print(data['feed']['opensearch:Query']['searchTerms'])
				print('Result size:' + data['feed']['opensearch:totalResults'])
				
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
			self.save_local_file(url=url, local_filename=file['name'] + '.zip')


	def save_local_file(self, url, local_filename):
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
				print('Fille already downloaded.')
		else:
			print('Error: ' + str(r))


