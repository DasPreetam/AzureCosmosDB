#Use pip install azure-cosmos
import azure.cosmos.cosmos_client as cosmos_client, ast, glob

try: 
	path = 'D:/Preetam/MeterRead/Output/*.json'   
	files=glob.glob(path)

	#Credentials
	config = {
		'ENDPOINT': 'https://tcs-utilities-itg-mongo.documents.azure.com',
		'PRIMARYKEY': 'mfX8o3OtlCQGoHqe68mMLWveXBJ7H05ObqRv9gYskP2LSODHAORZpdU9XXfiA7dKVkebHkZtBuU67PzrvuKsCg==',
		'DATABASE': 'cosmos',
		'CONTAINER': 'MeterRead'
	}

	#Initialize the cosmos client
	client = cosmos_client.CosmosClient(url_connection=config['ENDPOINT'],auth={'masterKey':config['PRIMARYKEY']})

	#Get the database link
	dbs = client.ReadDatabases()
	for item in dbs:
		if(item['id'] == config['DATABASE']):
			db_link=item['_self']
			break

	#Get the container link
	for item in client.ReadContainers(db_link):
		if(item['id'] == config['CONTAINER']):
			container_link=item['_self']
			break      

	#Read the data from files and upload
	for item in files :
		fileReader = open(item,'r')
		data_raw = fileReader.read()
		fileReader.close()

		upload=client.CreateItem(container_link,ast.literal_eval(data_raw))
		print('Upload Successful')

	print('Execution Complete')
	input('Hit Return key to exit')

except BaseException as e:
  print(str(e))
  input("Hit Return to exit")
